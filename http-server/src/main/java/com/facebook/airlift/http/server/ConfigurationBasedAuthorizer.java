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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static com.facebook.airlift.http.server.AuthorizationResult.failure;
import static com.facebook.airlift.http.server.AuthorizationResult.success;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.fromProperties;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ConfigurationBasedAuthorizer
        implements Authorizer
{
    private final Map<String, Pattern> roleRegexMap;

    @Inject
    public ConfigurationBasedAuthorizer(ConfigurationBasedAuthorizerConfig config)
            throws IOException
    {
        this(config.getRoleMapFilePath());
    }

    @VisibleForTesting
    public ConfigurationBasedAuthorizer(String roleMapFilePath)
            throws IOException
    {
        requireNonNull(roleMapFilePath, "roleMapFilePath is null");
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(Path.of(roleMapFilePath))) {
            properties.load(inputStream);
        }
        roleRegexMap = fromProperties(properties)
                .entrySet()
                .stream()
                .collect(toImmutableMap(Map.Entry::getKey, e -> Pattern.compile(e.getValue())));
    }

    @Override
    public AuthorizationResult authorize(Principal principal, Set<String> allowedRoles, String requestUrl)
    {
        for (String role : allowedRoles) {
            if (roleRegexMap.containsKey(role) && isPrincipalAuthorized(principal, roleRegexMap.get(role))) {
                return success();
            }
        }
        return failure(format("%s is not a member of the allowed roles: %s", principal.getName(), allowedRoles));
    }

    private boolean isPrincipalAuthorized(Principal principal, Pattern identityRegex)
    {
        return identityRegex.matcher(principal.getName()).matches();
    }
}
