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
package com.facebook.airlift.discovery.client;

import com.facebook.airlift.node.NodeInfo;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

import javax.inject.Provider;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DiscoveryModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        // bind service inventory
        binder.bind(ServiceInventory.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ServiceInventoryConfig.class);

        // for legacy configurations
        configBinder(binder).bindConfig(DiscoveryClientConfig.class);

        // bind discovery client and dependencies
        binder.bind(DiscoveryLookupClient.class).to(HttpDiscoveryLookupClient.class).in(Scopes.SINGLETON);
        binder.bind(DiscoveryAnnouncementClient.class).to(HttpDiscoveryAnnouncementClient.class).in(Scopes.SINGLETON);
        jsonCodecBinder(binder).bindJsonCodec(ServiceDescriptorsRepresentation.class);
        jsonCodecBinder(binder).bindJsonCodec(Announcement.class);

        // bind the http client
        httpClientBinder(binder).bindHttpClient("discovery", ForDiscoveryClient.class);

        // bind announcer
        binder.bind(Announcer.class).in(Scopes.SINGLETON);
        newExporter(binder).export(Announcer.class).withGeneratedName();

        // Must create a multibinder for service announcements or construction will fail if no
        // service announcements are bound, which is legal for processes that don't have public services
        newSetBinder(binder, ServiceAnnouncement.class);

        // bind selector factory
        binder.bind(CachingServiceSelectorFactory.class).in(Scopes.SINGLETON);
        binder.bind(ServiceSelectorFactory.class).to(MergingServiceSelectorFactory.class).in(Scopes.SINGLETON);

        binder.bind(ScheduledExecutorService.class)
                .annotatedWith(ForDiscoveryClient.class)
                .toProvider(DiscoveryExecutorProvider.class)
                .in(Scopes.SINGLETON);

        // bind selector manager with initial empty multibinder
        newSetBinder(binder, ServiceSelector.class);
        binder.bind(ServiceSelectorManager.class).in(Scopes.SINGLETON);

        newExporter(binder).export(ServiceInventory.class).withGeneratedName();
    }

    @Provides
    @ForDiscoveryClient
    public Supplier<URI> getDiscoveryUriSupplier(ServiceInventory serviceInventory, DiscoveryClientConfig config)
    {
        URI serviceUri = config.getDiscoveryServiceURI();

        return () -> {
            for (ServiceDescriptor descriptor : serviceInventory.getServiceDescriptors("discovery")) {
                if (descriptor.getState() != ServiceState.RUNNING) {
                    continue;
                }

                try {
                    return new URI(descriptor.getProperties().get("https"));
                }
                catch (Exception ignored) {
                }
                try {
                    return new URI(descriptor.getProperties().get("http"));
                }
                catch (Exception ignored) {
                }
            }

            return serviceUri;
        };
    }

    @Provides
    @Singleton
    public MergingServiceSelectorFactory createMergingServiceSelectorFactory(
            CachingServiceSelectorFactory factory,
            Announcer announcer,
            NodeInfo nodeInfo)
    {
        return new MergingServiceSelectorFactory(factory, announcer, nodeInfo);
    }

    private static class DiscoveryExecutorProvider
            implements Provider<ScheduledExecutorService>
    {
        private ScheduledExecutorService executor;

        @Override
        public ScheduledExecutorService get()
        {
            checkState(executor == null, "provider already used");
            executor = new ScheduledThreadPoolExecutor(5, daemonThreadsNamed("Discovery-%s"));
            return executor;
        }

        @PreDestroy
        public void destroy()
        {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }
}
