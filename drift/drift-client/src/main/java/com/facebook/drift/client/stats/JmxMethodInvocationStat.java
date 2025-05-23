/*
 * Copyright (C) 2012 Facebook, Inc.
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
package com.facebook.drift.client.stats;

import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TimeStat;
import com.google.common.util.concurrent.ListenableFuture;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static com.facebook.airlift.units.Duration.nanosSince;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JmxMethodInvocationStat
        implements MethodInvocationStat
{
    private final String name;
    private final TimeStat time = new TimeStat(MILLISECONDS);
    private final CounterStat successes = new CounterStat();
    private final CounterStat failures = new CounterStat();
    private final CounterStat retries = new CounterStat();

    public JmxMethodInvocationStat(String name)
    {
        this.name = requireNonNull(name, "name is null");
    }

    public String getName()
    {
        return name;
    }

    @Managed
    @Nested
    public TimeStat getTime()
    {
        return time;
    }

    @Managed
    @Nested
    public CounterStat getSuccesses()
    {
        return successes;
    }

    @Managed
    @Nested
    public CounterStat getFailures()
    {
        return failures;
    }

    @Managed
    @Nested
    public CounterStat getRetries()
    {
        return retries;
    }

    @Override
    public void recordResult(long startTime, ListenableFuture<Object> result)
    {
        result.addListener(
                () -> {
                    time.add(nanosSince(startTime));
                    try {
                        result.get();
                        successes.update(1);
                    }
                    catch (Throwable throwable) {
                        failures.update(1);
                    }
                },
                directExecutor());
    }

    @Override
    public void recordRetry()
    {
        retries.update(1);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("successes", successes.getTotalCount())
                .add("failures", failures.getTotalCount())
                .add("retries", retries.getTotalCount())
                .toString();
    }
}
