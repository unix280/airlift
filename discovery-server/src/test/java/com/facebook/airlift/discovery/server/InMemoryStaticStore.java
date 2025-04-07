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
package com.facebook.airlift.discovery.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.facebook.airlift.discovery.server.Service.matchesPool;
import static com.facebook.airlift.discovery.server.Service.matchesType;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class InMemoryStaticStore
        implements StaticStore
{
    private final Map<Id<Service>, Service> services = new HashMap<>();

    @Override
    public synchronized void put(Service service)
    {
        requireNonNull(service, "service is null");
        Preconditions.checkArgument(service.getNodeId() == null, "service.nodeId should be null");

        services.put(service.getId(), service);
    }

    @Override
    public synchronized void delete(Id<Service> id)
    {
        services.remove(id);
    }

    @Override
    public synchronized Set<Service> getAll()
    {
        return ImmutableSet.copyOf(services.values());
    }

    @Override
    public synchronized Set<Service> get(String type)
    {
        return getAll().stream().filter(service -> matchesType(type).test(service)).collect(toImmutableSet());
    }

    @Override
    public synchronized Set<Service> get(String type, String pool)
    {
        return getAll().stream()
                .filter(service -> matchesType(type).test(service) && matchesPool(pool).test(service))
                .collect(toImmutableSet());
    }
}
