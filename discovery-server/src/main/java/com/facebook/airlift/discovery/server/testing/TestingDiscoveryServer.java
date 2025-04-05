/*
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
package com.facebook.airlift.discovery.server.testing;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.discovery.client.DiscoveryModule;
import com.facebook.airlift.discovery.server.EmbeddedDiscoveryModule;
import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.jmx.testing.TestingJmxModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.google.inject.Injector;
import org.weakref.jmx.guice.MBeanModule;

import java.net.URI;

public class TestingDiscoveryServer
        implements AutoCloseable
{
    private final LifeCycleManager lifeCycleManager;
    private final TestingHttpServer server;

    public TestingDiscoveryServer(String environment)
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new MBeanModule(),
                new TestingNodeModule(environment),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(true),
                new TestingJmxModule(),
                new DiscoveryModule(),
                new EmbeddedDiscoveryModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperty("discovery.store-cache-ttl", "0ms")
                .quiet()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        server = injector.getInstance(TestingHttpServer.class);
    }

    public URI getBaseUrl()
    {
        return server.getBaseUrl();
    }

    @Override
    public void close()
            throws Exception
    {
        if (lifeCycleManager != null) {
            lifeCycleManager.stop();
        }
    }
}
