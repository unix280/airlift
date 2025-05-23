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

import com.facebook.airlift.discovery.store.DistributedStore;
import com.facebook.airlift.discovery.store.Entry;
import com.facebook.airlift.json.JsonCodec;
import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;

import java.util.Set;

import static com.facebook.airlift.discovery.server.Service.matchesPool;
import static com.facebook.airlift.discovery.server.Service.matchesType;
import static java.util.Objects.requireNonNull;

public class ReplicatedStaticStore
        implements StaticStore
{
    private final DistributedStore store;
    private final JsonCodec<Service> codec;

    @Inject
    public ReplicatedStaticStore(@ForStaticStore DistributedStore store, JsonCodec<Service> codec)
    {
        this.store = requireNonNull(store, "store is null");
        this.codec = requireNonNull(codec, "codec is null");
    }

    @Override
    public void put(Service service)
    {
        byte[] key = service.getId().getBytes();
        byte[] value = codec.toJsonBytes(service);

        store.put(key, value);
    }

    @Override
    public void delete(Id<Service> id)
    {
        store.delete(id.getBytes());
    }

    @Override
    public Set<Service> getAll()
    {
        ImmutableSet.Builder<Service> builder = ImmutableSet.builder();
        for (Entry entry : store.getAll()) {
            builder.add(codec.fromJson(entry.getValue()));
        }

        return builder.build();
    }

    @Override
    public Set<Service> get(String type)
    {
        return ImmutableSet.copyOf(getAll().stream().filter(matchesType(type)).iterator());
    }

    @Override
    public Set<Service> get(String type, String pool)
    {
        return ImmutableSet.copyOf(getAll().stream()
                .filter(service -> matchesType(type).test(service) && matchesPool(pool).test(service))
                .iterator());
    }
}
