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

import jakarta.inject.Inject;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.util.resource.PathResourceFactory;

import javax.inject.Provider;

import static com.google.common.base.Strings.isNullOrEmpty;

public class HashLoginServiceProvider
        implements Provider<HashLoginService>
{
    private final HttpServerConfig config;
    private final PathResourceFactory pathResourceFactory = new PathResourceFactory();

    @Inject
    public HashLoginServiceProvider(HttpServerConfig config)
    {
        this.config = config;
    }

    @Override
    public HashLoginService get()
    {
        String authConfig = config.getUserAuthFile();
        if (!isNullOrEmpty(authConfig)) {
            return new HashLoginService(HttpServerModule.REALM_NAME, pathResourceFactory.newResource(authConfig));
        }
        return null;
    }
}
