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

import com.facebook.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

public class ConfigurationBasedAuthorizerConfig
{
    private String roleMapFilePath;

    @NotNull
    public String getRoleMapFilePath()
    {
        return roleMapFilePath;
    }

    @Config("configuration-based-authorizer.role-regex-map.file-path")
    public ConfigurationBasedAuthorizerConfig setRoleMapFilePath(String roleMapFilePath)
    {
        this.roleMapFilePath = roleMapFilePath;
        return this;
    }
}
