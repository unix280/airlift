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
package com.facebook.airlift.jaxrs;

import com.facebook.airlift.http.server.AuthorizationResult;
import com.facebook.airlift.http.server.Authorizer;
import com.facebook.airlift.http.server.HttpServerConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.http.server.HttpServerConfig.AuthorizationPolicy;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Provider
public class AuthorizationFilter
        implements ContainerRequestFilter
{
    private final Authorizer authorizer;
    private final AuthorizationPolicy authorizationPolicy;
    private final Set<String> defaultAllowedRoles;
    private final Map<Class<?>, Map<String, String>> roleMaps;
    private final boolean allowUnsecureRequestsInAuthorizer;

    @Context
    private ResourceInfo resourceInfo;

    @Inject
    public AuthorizationFilter(
            Authorizer authorizer,
            HttpServerConfig httpServerConfig,
            @RoleMapping Map<Class<?>, Map<String, String>> roleMaps)
    {
        this(
                authorizer,
                httpServerConfig.getDefaultAuthorizationPolicy(),
                httpServerConfig.getDefaultAllowedRoles(),
                roleMaps,
                httpServerConfig.isAllowUnsecureRequestsInAuthorizer());
    }

    @VisibleForTesting
    public AuthorizationFilter(
            Authorizer authorizer,
            AuthorizationPolicy authorizationPolicy,
            Set<String> defaultAllowedRoles,
            Map<Class<?>, Map<String, String>> roleMaps,
            boolean allowUnsecureRequestsInAuthorizer)
    {
        this.authorizer = requireNonNull(authorizer, "authorizer is null");
        this.authorizationPolicy = requireNonNull(authorizationPolicy, "authorizationPolicy is null");
        this.defaultAllowedRoles = requireNonNull(defaultAllowedRoles, "defaultAllowedRoles is null");
        this.roleMaps = requireNonNull(roleMaps, "roleMaps is null");
        this.allowUnsecureRequestsInAuthorizer = allowUnsecureRequestsInAuthorizer;
    }

    @Override
    public void filter(ContainerRequestContext request)
    {
        // skip authorization if non-secure
        if (!request.getSecurityContext().isSecure() && allowUnsecureRequestsInAuthorizer) {
            return;
        }

        Principal principal = request.getSecurityContext().getUserPrincipal();
        if (principal == null) {
            request.abortWith(Response.status(Response.Status.FORBIDDEN)
                    .entity("Request principal is missing.")
                    .build());
            return;
        }

        Optional<Set<String>> allowedRoles = getAllowedRoles();
        if (!allowedRoles.isPresent()) {
            switch (authorizationPolicy) {
                case ALLOW:
                    return;
                case DENY:
                    request.abortWith(Response.status(Response.Status.FORBIDDEN)
                            .entity(format("Principal %s is not allowed to access the resource. Reason: denied by default policy",
                                    principal.getName()))
                            .build());
                    return;
                case DEFAULT_ROLES:
                    allowedRoles = Optional.of(defaultAllowedRoles);
                    break;
                default:
            }
        }
        else if (roleMaps.containsKey(resourceInfo.getResourceClass())) {
            allowedRoles = Optional.of(allowedRoles.orElseThrow()
                    .stream()
                    .map(role -> roleMaps.get(resourceInfo.getResourceClass()).getOrDefault(role, role))
                    .collect(toImmutableSet()));
        }

        AuthorizationResult result = authorizer.authorize(
                principal,
                allowedRoles.orElseThrow(),
                request.getUriInfo().getRequestUri().toString());
        if (!result.isAllowed()) {
            request.abortWith(Response.status(Response.Status.FORBIDDEN)
                    .entity(format("Principal %s is not allowed to access the resource. Reason: %s",
                            principal.getName(),
                            result.getReason()))
                    .build());
        }
    }

    private Optional<Set<String>> getAllowedRoles()
    {
        // Checking method level annotation for roles (overrides the class level annotation if exists)
        if (resourceInfo.getResourceMethod().isAnnotationPresent(RolesAllowed.class)) {
            return Optional.of(ImmutableSet.copyOf(resourceInfo.getResourceMethod().getAnnotation(RolesAllowed.class).value()));
        }
        // Checking class level annotation for roles
        else if (resourceInfo.getResourceClass().isAnnotationPresent(RolesAllowed.class)) {
            return Optional.of(ImmutableSet.copyOf(resourceInfo.getResourceClass().getAnnotation(RolesAllowed.class).value()));
        }
        else {
            return Optional.empty();
        }
    }
}
