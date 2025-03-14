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
package com.facebook.airlift.http.server;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.StatusResponseHandler.StatusResponse;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.Servlet;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.core.Response;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.airlift.http.server.AuthorizationResult.failure;
import static com.facebook.airlift.http.server.AuthorizationResult.success;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.testng.Assert.assertEquals;

public class TestAuthorizationEnabledServletInHttpServer
{
    private HttpClient client;

    @BeforeClass
    public void setup()
    {
        client = new JettyHttpClient();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(client);
    }

    @Test
    public void testAuthDisabled()
            throws Exception
    {
        Map<String, String> serverProperties = ImmutableMap.of("http-server.authorization.enabled", "false");
        TestingHttpServer server = createServer(serverProperties);
        server.start();
        assertEquals(sendRequest(server, "unmarked").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequest(server, "user").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequest(server, "admin").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "unmarked", "user").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "user", "user").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "admin", "user").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "unmarked", "admin").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "user", "admin").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "admin", "admin").getStatusCode(), Response.Status.OK.getStatusCode());
        server.stop();
    }

    @Test
    public void testDefaultPolicyAllow()
            throws Exception
    {
        Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .put("http-server.authorization.enabled", "true")
                .put("http-server.authorization.default-policy", "ALLOW")
                .build();
        TestingHttpServer server = createServer(serverProperties);
        server.start();
        assertEquals(sendRequest(server, "unmarked").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequest(server, "user").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequest(server, "admin").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequestWithRole(server, "unmarked", "user").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "user", "user").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "admin", "user").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequestWithRole(server, "unmarked", "admin").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "user", "admin").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequestWithRole(server, "admin", "admin").getStatusCode(), Response.Status.OK.getStatusCode());
        server.stop();
    }

    @Test
    public void testDefaultPolicyDeny()
            throws Exception
    {
        Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .put("http-server.authorization.enabled", "true")
                .put("http-server.authorization.default-policy", "DENY")
                .build();
        TestingHttpServer server = createServer(serverProperties);
        server.start();
        assertEquals(sendRequest(server, "unmarked").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequest(server, "user").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequest(server, "admin").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequestWithRole(server, "unmarked", "user").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequestWithRole(server, "user", "user").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "admin", "user").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequestWithRole(server, "unmarked", "admin").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequestWithRole(server, "user", "admin").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequestWithRole(server, "admin", "admin").getStatusCode(), Response.Status.OK.getStatusCode());
        server.stop();
    }

    @Test
    public void testDefaultPolicyDefaultRoles()
            throws Exception
    {
        Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .put("http-server.authorization.enabled", "true")
                .put("http-server.authorization.default-policy", "DEFAULT_ROLES")
                .put("http-server.authorization.default-allowed-roles", "internal, admin")
                .build();
        TestingHttpServer server = createServer(serverProperties);
        server.start();
        assertEquals(sendRequest(server, "unmarked").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequest(server, "user").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequest(server, "admin").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequestWithRole(server, "unmarked", "user").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequestWithRole(server, "user", "user").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "admin", "user").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequestWithRole(server, "unmarked", "admin").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "user", "admin").getStatusCode(), Response.Status.FORBIDDEN.getStatusCode());
        assertEquals(sendRequestWithRole(server, "admin", "admin").getStatusCode(), Response.Status.OK.getStatusCode());
        server.stop();
    }

    @Test
    public void testAllowUnsecureRequestsInAuthorizer()
            throws Exception
    {
        Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .put("http-server.authorization.enabled", "true")
                .put("http-server.authorization.default-policy", "DEFAULT_ROLES")
                .put("http-server.authorization.default-allowed-roles", "internal, admin")
                .put("http-server.authorization.allow-unsecured-requests", "true")
                .build();
        TestingHttpServer server = createServer(serverProperties);
        server.start();
        assertEquals(sendRequest(server, "unmarked").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequest(server, "user").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequest(server, "admin").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "unmarked", "user").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "user", "user").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "admin", "user").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "unmarked", "admin").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "user", "admin").getStatusCode(), Response.Status.OK.getStatusCode());
        assertEquals(sendRequestWithRole(server, "admin", "admin").getStatusCode(), Response.Status.OK.getStatusCode());
        server.stop();
    }

    private StatusResponse sendRequest(TestingHttpServer server, String resource)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl()).appendPath(resource).build();
        Request request = Request.Builder.prepareGet().setUri(uri).build();
        return client.execute(request, createStatusResponseHandler());
    }

    private StatusResponse sendRequestWithRole(TestingHttpServer server, String resource, String role)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl()).appendPath(resource).build();
        Request request = Request.Builder.prepareGet().setUri(uri).setHeader("ROLE", role).build();
        return client.execute(request, createStatusResponseHandler());
    }

    private static class MockAuthorizer
            implements Authorizer
    {
        @Override
        public AuthorizationResult authorize(Principal principal, Set<String> allowedRoles, String requestUrl)
        {
            for (String role : allowedRoles) {
                if (role.equals(principal.getName())) {
                    return success();
                }
            }
            return failure("Not an allowed role");
        }
    }

    private static class MockAuthenticationFilter
            implements Filter
    {
        @Override
        public void init(FilterConfig filterConfig)
        {
        }

        @Override
        public void destroy()
        {
        }

        @Override
        public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain nextFilter)
                throws IOException, ServletException
        {
            HttpServletRequest request = (HttpServletRequest) servletRequest;
            HttpServletResponse response = (HttpServletResponse) servletResponse;

            String role = request.getHeader("ROLE");
            if (role != null) {
                request = new HttpServletRequestWrapper(request)
                {
                    @Override
                    public Principal getUserPrincipal()
                    {
                        return () -> role;
                    }
                };
            }
            nextFilter.doFilter(request, response);
        }
    }

    public static class MockUnmarkedServlet
            extends HttpServlet
    {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
        {
            response.setStatus(HttpServletResponse.SC_OK);
        }
    }

    @RolesAllowed("user")
    public static class MockUserServlet
            extends HttpServlet
    {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
        {
            response.setStatus(HttpServletResponse.SC_OK);
        }
    }

    @RolesAllowed("admin")
    public static class MockAdminServlet
            extends HttpServlet
    {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
        {
            response.setStatus(HttpServletResponse.SC_OK);
        }
    }

    private static TestingHttpServer createServer(Map<String, String> serverProperties)
    {
        List<Module> modules = ImmutableList.<Module>builder()
                .add(new TestingNodeModule())
                .add(new TestingHttpServerModule())
                .add(new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        binder.bind(Servlet.class).annotatedWith(TheServlet.class).to(DummyServlet.class);
                        newSetBinder(binder, Filter.class, TheServlet.class).addBinding()
                                .to(MockAuthenticationFilter.class).in(Scopes.SINGLETON);
                        newMapBinder(binder, String.class, Servlet.class, TheServlet.class)
                                .addBinding("/unmarked/*")
                                .to(MockUnmarkedServlet.class)
                                .in(Scopes.SINGLETON);
                        newMapBinder(binder, String.class, Servlet.class, TheServlet.class)
                                .addBinding("/user/*")
                                .to(MockUserServlet.class)
                                .in(Scopes.SINGLETON);
                        newMapBinder(binder, String.class, Servlet.class, TheServlet.class)
                                .addBinding("/admin/*")
                                .to(MockAdminServlet.class)
                                .in(Scopes.SINGLETON);
                    }

                    @Provides
                    @TheServlet
                    public Map<String, String> createTheServletParams()
                    {
                        return new HashMap<>();
                    }
                })
                .add(installModuleIf(
                        HttpServerConfig.class,
                        HttpServerConfig::isAuthorizationEnabled,
                        binder -> binder.bind(Authorizer.class).to(MockAuthorizer.class)))
                .build();

        return new Bootstrap(modules)
                .doNotInitializeLogging()
                .setOptionalConfigurationProperties(serverProperties)
                .quiet()
                .initialize()
                .getInstance(TestingHttpServer.class);
    }
}
