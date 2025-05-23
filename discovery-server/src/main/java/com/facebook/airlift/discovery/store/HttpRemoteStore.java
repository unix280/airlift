/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.airlift.discovery.store;

import com.facebook.airlift.discovery.client.ServiceDescriptor;
import com.facebook.airlift.discovery.client.ServiceSelector;
import com.facebook.airlift.http.client.BodyGenerator;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.airlift.units.Duration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;

import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.inject.name.Names.named;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.weakref.jmx.ObjectNames.generatedNameOf;

public class HttpRemoteStore
        implements RemoteStore
{
    private static final Logger log = Logger.get(HttpRemoteStore.class);

    private final int maxBatchSize;
    private final int queueSize;
    private final Duration updateInterval;

    private final ConcurrentMap<String, BatchProcessor<Entry>> processors = new ConcurrentHashMap<>();
    private final String name;
    private final NodeInfo node;
    private final ServiceSelector selector;
    private final HttpClient httpClient;

    private Future<?> future;
    private ScheduledExecutorService executor;

    private final AtomicLong lastRemoteServerRefreshTimestamp = new AtomicLong();
    private final MBeanExporter mbeanExporter;

    @Inject
    public HttpRemoteStore(String name,
            NodeInfo node,
            ServiceSelector selector,
            StoreConfig config,
            HttpClient httpClient,
            MBeanExporter mbeanExporter)
    {
        requireNonNull(name, "name is null");
        requireNonNull(node, "node is null");
        requireNonNull(selector, "selector is null");
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(config, "config is null");
        requireNonNull(mbeanExporter, "mBeanExporter is null");

        this.name = name;
        this.node = node;
        this.selector = selector;
        this.httpClient = httpClient;
        this.mbeanExporter = mbeanExporter;

        maxBatchSize = config.getMaxBatchSize();
        queueSize = config.getQueueSize();
        updateInterval = config.getRemoteUpdateInterval();
    }

    @PostConstruct
    public synchronized void start()
    {
        if (future == null) {
            // note: this *must* be single threaded for the shutdown logic to work correctly
            executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("http-remote-store-" + name));

            future = executor.scheduleWithFixedDelay(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        updateProcessors(selector.selectAllServices());
                    }
                    catch (Throwable e) {
                        log.warn(e, "Error refreshing batch processors");
                    }
                }
            }, 0, updateInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @PreDestroy
    public synchronized void shutdown()
    {
        if (future != null) {
            future.cancel(true);

            try {
                // schedule a task to shut down all processors and wait for it to complete. We rely on the executor
                // having a *single* thread to guarantee the execution happens after any currently running task
                // (in case the cancel call above didn't do its magic and the scheduled task is still running)
                executor.submit(new Runnable()
                {
                    public void run()
                    {
                        updateProcessors(Collections.<ServiceDescriptor>emptyList());
                    }
                }).get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (ExecutionException e) {
                throw new RuntimeException(e);
            }

            executor.shutdownNow();

            future = null;
        }
    }

    private void updateProcessors(List<ServiceDescriptor> descriptors)
    {
        Set<String> nodeIds = descriptors.stream().map(getNodeIdFunction()).collect(toImmutableSet());

        // remove old ones
        Iterator<Map.Entry<String, BatchProcessor<Entry>>> iterator = processors.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, BatchProcessor<Entry>> entry = iterator.next();

            if (!nodeIds.contains(entry.getKey())) {
                iterator.remove();
                entry.getValue().stop();
                mbeanExporter.unexport(nameFor(entry.getKey()));
            }
        }

        Iterable<ServiceDescriptor> newDescriptors = descriptors.stream()
                .filter(descriptor ->
                        !descriptor.getNodeId().equals(node.getNodeId())
                                && !processors.containsKey(descriptor.getNodeId()))
                .collect(toImmutableList());

        for (ServiceDescriptor descriptor : newDescriptors) {
            BatchProcessor<Entry> processor = new BatchProcessor<Entry>(descriptor.getNodeId(),
                    new MyBatchHandler(name, descriptor, httpClient),
                    maxBatchSize,
                    queueSize);

            processor.start();
            processors.put(descriptor.getNodeId(), processor);
            mbeanExporter.export(nameFor(descriptor.getNodeId()), processor);
        }

        lastRemoteServerRefreshTimestamp.set(System.currentTimeMillis());
    }

    private String nameFor(String id)
    {
        return generatedNameOf(BatchProcessor.class, named(name + "-" + id));
    }

    @Managed
    public long getLastRemoteServerRefreshTimestamp()
    {
        return lastRemoteServerRefreshTimestamp.get();
    }

    private static Function<ServiceDescriptor, String> getNodeIdFunction()
    {
        return new Function<ServiceDescriptor, String>()
        {
            public String apply(ServiceDescriptor descriptor)
            {
                return descriptor.getNodeId();
            }
        };
    }

    @Override
    public void put(Entry entry)
    {
        for (BatchProcessor<Entry> processor : processors.values()) {
            processor.put(entry);
        }
    }

    private static class MyBatchHandler
            implements BatchProcessor.BatchHandler<Entry>
    {
        private final ObjectMapper mapper = new ObjectMapper(new SmileFactory());

        private final URI uri;
        private final HttpClient httpClient;

        public MyBatchHandler(String name, ServiceDescriptor descriptor, HttpClient httpClient)
        {
            this.httpClient = httpClient;

            // TODO: build URI from resource class
            if (descriptor.getProperties().get("https") != null) {
                uri = URI.create(descriptor.getProperties().get("https") + "/v1/store/" + name);
            }
            else {
                uri = URI.create(descriptor.getProperties().get("http") + "/v1/store/" + name);
            }
        }

        @Override
        public void processBatch(final Collection<Entry> entries)
        {
            final Request request = Request.Builder.preparePost()

                    .setUri(uri)
                    .setHeader("Content-Type", "application/x-jackson-smile")
                    .setBodyGenerator(new BodyGenerator()
                    {
                        @Override
                        public void write(OutputStream out)
                                throws Exception
                        {
                            mapper.writeValue(out, entries);
                        }
                    })
                    .build();

            try {
                httpClient.execute(request, createStatusResponseHandler());
            }
            catch (RuntimeException e) {
                // ignore
            }
        }
    }
}
