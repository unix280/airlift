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

import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.node.NodeInfo;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestServiceInventory
{
    @Test
    public void testNullServiceInventory()
    {
        try (JettyHttpClient httpClient = new JettyHttpClient()) {
            ServiceInventory serviceInventory = new ServiceInventory(new ServiceInventoryConfig(),
                    new NodeInfo("test"),
                    JsonCodec.jsonCodec(ServiceDescriptorsRepresentation.class),
                    httpClient);

            assertEquals(Iterables.size(serviceInventory.getServiceDescriptors()), 0);
            serviceInventory.updateServiceInventory();
            assertEquals(Iterables.size(serviceInventory.getServiceDescriptors()), 0);
        }
    }

    @Test
    public void testFileServiceInventory()
            throws Exception
    {
        try (JettyHttpClient httpClient = new JettyHttpClient()) {
            ServiceInventoryConfig serviceInventoryConfig = new ServiceInventoryConfig()
                    .setServiceInventoryUri(Resources.getResource("service-inventory.json").toURI());

            ServiceInventory serviceInventory = new ServiceInventory(serviceInventoryConfig,
                    new NodeInfo("test"),
                    JsonCodec.jsonCodec(ServiceDescriptorsRepresentation.class),
                    httpClient);

            assertEquals(Iterables.size(serviceInventory.getServiceDescriptors()), 2);
            assertEquals(Iterables.size(serviceInventory.getServiceDescriptors("discovery")), 2);
            assertEquals(Iterables.size(serviceInventory.getServiceDescriptors("discovery", "general")), 2);
            serviceInventory.updateServiceInventory();
            assertEquals(Iterables.size(serviceInventory.getServiceDescriptors()), 2);
            assertEquals(Iterables.size(serviceInventory.getServiceDescriptors("discovery")), 2);
            assertEquals(Iterables.size(serviceInventory.getServiceDescriptors("discovery", "general")), 2);
        }
    }

    @Test
    public void testHttpServiceInventory()
            throws Exception
    {
        String serviceInventoryJson = Resources.toString(Resources.getResource("service-inventory.json"), UTF_8);

        Server server = null;
        try (JettyHttpClient httpClient = new JettyHttpClient()) {
            int port;
            try (ServerSocket socket = new ServerSocket()) {
                socket.bind(new InetSocketAddress(0));
                port = socket.getLocalPort();
            }
            URI baseURI = new URI("http", null, "127.0.0.1", port, null, null, null);

            HttpConfiguration httpConfiguration = new HttpConfiguration();
            httpConfiguration.setSendServerVersion(false);
            httpConfiguration.setSendXPoweredBy(false);

            server = new Server();

            ServerConnector httpConnector = new ServerConnector(server, new HttpConnectionFactory(httpConfiguration));
            httpConnector.setPort(port);
            httpConnector.setName("http");
            server.addConnector(httpConnector);

            ServletHolder servletHolder = new ServletHolder(new ServiceInventoryServlet(serviceInventoryJson));
            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
            context.addServlet(servletHolder, "/*");
            ContextHandlerCollection handlers = new ContextHandlerCollection();
            handlers.addHandler(context);
            server.setHandler(handlers);

            server.start();

            // test
            ServiceInventoryConfig serviceInventoryConfig = new ServiceInventoryConfig()
                    .setServiceInventoryUri(baseURI);

            ServiceInventory serviceInventory = new ServiceInventory(serviceInventoryConfig,
                    new NodeInfo("test"),
                    JsonCodec.jsonCodec(ServiceDescriptorsRepresentation.class),
                    httpClient);

            assertEquals(Iterables.size(serviceInventory.getServiceDescriptors()), 2);
            assertEquals(Iterables.size(serviceInventory.getServiceDescriptors("discovery")), 2);
            assertEquals(Iterables.size(serviceInventory.getServiceDescriptors("discovery", "general")), 2);
            serviceInventory.updateServiceInventory();
            assertEquals(Iterables.size(serviceInventory.getServiceDescriptors()), 2);
            assertEquals(Iterables.size(serviceInventory.getServiceDescriptors("discovery")), 2);
            assertEquals(Iterables.size(serviceInventory.getServiceDescriptors("discovery", "general")), 2);
        }
        finally {
            if (server != null) {
                server.stop();
            }
        }
    }

    private static class ServiceInventoryServlet
            extends HttpServlet
    {
        private final byte[] serviceInventory;

        private ServiceInventoryServlet(String serviceInventory)
        {
            this.serviceInventory = serviceInventory.getBytes(UTF_8);
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            response.setHeader("Content-Type", "application/json");
            response.setStatus(200);
            response.getOutputStream().write(serviceInventory);
        }
    }
}
