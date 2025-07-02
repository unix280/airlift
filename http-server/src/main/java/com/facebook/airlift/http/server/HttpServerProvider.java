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

import com.facebook.airlift.event.client.EventClient;
import com.facebook.airlift.http.server.HttpServerBinder.HttpResourceBinding;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.airlift.tracetoken.TraceTokenManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import jakarta.annotation.Nullable;
import jakarta.servlet.Filter;
import jakarta.servlet.Servlet;
import org.eclipse.jetty.security.LoginService;

import javax.inject.Provider;
import javax.management.MBeanServer;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

/**
 * Provides an instance of a Jetty server ready to be configured with
 * com.google.inject.servlet.ServletModule
 */
public class HttpServerProvider
        implements Provider<HttpServer>
{
    private final HttpServerInfo httpServerInfo;
    private final NodeInfo nodeInfo;
    private final HttpServerConfig config;
    private final Servlet defaultServlet;
    private final Map<String, Servlet> servlets;
    private final Set<HttpResourceBinding> resources;
    private Map<String, String> servletInitParameters = ImmutableMap.of();
    private Servlet theAdminServlet;
    private Map<String, String> adminServletInitParameters = ImmutableMap.of();
    private MBeanServer mbeanServer;
    private LoginService loginService;
    private final RequestStats stats;
    private final Set<Filter> filters;
    private final Set<Filter> adminFilters;
    private TraceTokenManager traceTokenManager;
    private final EventClient eventClient;
    private Authorizer authorizer;

    @Inject
    public HttpServerProvider(HttpServerInfo httpServerInfo,
            NodeInfo nodeInfo,
            HttpServerConfig config,
            @TheServlet Servlet defaultServlet,
            @TheServlet Map<String, Servlet> servlets,
            @TheServlet Set<Filter> filters,
            @TheServlet Set<HttpResourceBinding> resources,
            @TheAdminServlet Set<Filter> adminFilters,
            RequestStats stats,
            EventClient eventClient)
    {
        requireNonNull(httpServerInfo, "httpServerInfo is null");
        requireNonNull(nodeInfo, "nodeInfo is null");
        requireNonNull(config, "config is null");
        requireNonNull(defaultServlet, "defaultServlet is null");
        requireNonNull(servlets, "servlets is null");
        requireNonNull(filters, "filters is null");
        requireNonNull(resources, "resources is null");
        requireNonNull(adminFilters, "adminFilters is null");
        requireNonNull(stats, "stats is null");
        requireNonNull(eventClient, "eventClient is null");

        this.httpServerInfo = httpServerInfo;
        this.nodeInfo = nodeInfo;
        this.config = config;
        this.defaultServlet = defaultServlet;
        this.servlets = servlets;
        this.filters = ImmutableSet.copyOf(filters);
        this.resources = ImmutableSet.copyOf(resources);
        this.adminFilters = ImmutableSet.copyOf(adminFilters);
        this.stats = stats;
        this.eventClient = eventClient;
    }

    @Inject(optional = true)
    public void setServletInitParameters(@TheServlet Map<String, String> parameters)
    {
        this.servletInitParameters = ImmutableMap.copyOf(parameters);
    }

    @Inject(optional = true)
    public void setTheAdminServlet(@TheAdminServlet Servlet theAdminServlet)
    {
        this.theAdminServlet = theAdminServlet;
    }

    @Inject(optional = true)
    public void setAdminServletInitParameters(@TheAdminServlet Map<String, String> parameters)
    {
        this.adminServletInitParameters = ImmutableMap.copyOf(parameters);
    }

    @Inject(optional = true)
    public void setMBeanServer(MBeanServer server)
    {
        mbeanServer = server;
    }

    @Inject(optional = true)
    public void setLoginService(@Nullable LoginService loginService)
    {
        this.loginService = loginService;
    }

    @Inject(optional = true)
    public void setTokenManager(@Nullable TraceTokenManager tokenManager)
    {
        this.traceTokenManager = tokenManager;
    }

    @Inject(optional = true)
    public void setAuthorizer(@Nullable Authorizer authorizer)
    {
        this.authorizer = authorizer;
    }

    @Override
    public HttpServer get()
    {
        try {
            HttpServer httpServer = new HttpServer(httpServerInfo,
                    nodeInfo,
                    config,
                    defaultServlet,
                    servlets,
                    servletInitParameters,
                    filters,
                    resources,
                    theAdminServlet,
                    adminServletInitParameters,
                    adminFilters,
                    mbeanServer,
                    loginService,
                    traceTokenManager,
                    stats,
                    eventClient,
                    authorizer);
            httpServer.start();
            return httpServer;
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
