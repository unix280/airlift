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

import com.facebook.airlift.units.Duration;

import java.time.ZonedDateTime;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;

public class TestReplicatedDynamicStoreWithTtl
        extends TestReplicatedDynamicStore
{
    @Override
    protected DynamicStore initializeStore(DiscoveryConfig config, Supplier<ZonedDateTime> timeSupplier)
    {
        // Tests for initialization of the memoized supplier with Ttl
        config.setStoreCacheTtl(Duration.succinctDuration(1000, SECONDS));
        return super.initializeStore(config, timeSupplier);
    }

    @Override
    public void testDelete()
    {
        // ignore; test relies on not having the cached service supplier
    }
}
