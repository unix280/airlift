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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.facebook.airlift.discovery.server.DynamicServiceAnnouncement.toServiceWith;
import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.concat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class TestDynamicStore
{
    private static final Duration MAX_AGE = new Duration(1, TimeUnit.MINUTES);

    protected TestingTimeSupplier currentTime;
    protected DynamicStore store;

    protected abstract DynamicStore initializeStore(DiscoveryConfig config, Supplier<ZonedDateTime> timeSupplier);

    @BeforeMethod
    public void setup()
    {
        currentTime = new TestingTimeSupplier();
        DiscoveryConfig config = new DiscoveryConfig()
                .setMaxAge(new Duration(1, TimeUnit.MINUTES))
                .setStoreCacheTtl(new Duration(0, TimeUnit.SECONDS));
        store = initializeStore(config, currentTime);
    }

    @Test
    public void testEmpty()
    {
        assertTrue(store.getAll().isEmpty(), "store should be empty");
    }

    @Test
    public void testPutSingle()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))));

        store.put(nodeId, blue);

        assertEquals(store.getAll(), blue.getServiceAnnouncements().stream()
                .map(toServiceWith(nodeId, blue.getLocation(), blue.getPool()))
                .collect(toImmutableList()));
    }

    @Test
    public void testExpires()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))));

        store.put(nodeId, blue);
        advanceTimeBeyondMaxAge();
        assertEquals(store.getAll(), Collections.<Service>emptySet());
    }

    @Test
    public void testPutMultipleForSameNode()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111")),
                new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("http", "http://localhost:2222")),
                new DynamicServiceAnnouncement(Id.<Service>random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))));

        store.put(nodeId, announcement);

        assertEqualsIgnoreOrder(store.getAll(), announcement.getServiceAnnouncements().stream()
                .map(toServiceWith(nodeId, announcement.getLocation(), announcement.getPool()))
                .collect(toImmutableList()));
    }

    @Test
    public void testReplace()
    {
        Id<Node> nodeId = Id.random();

        DynamicAnnouncement oldAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))));

        DynamicAnnouncement newAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))));

        store.put(nodeId, oldAnnouncement);
        currentTime.increment();
        store.put(nodeId, newAnnouncement);

        assertEquals(store.getAll(), newAnnouncement.getServiceAnnouncements().stream()
                .map(toServiceWith(nodeId, newAnnouncement.getLocation(), newAnnouncement.getPool()))
                .collect(toImmutableList()));
    }

    @Test
    public void testReplaceExpired()
    {
        Id<Node> nodeId = Id.random();

        DynamicAnnouncement oldAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))));

        DynamicAnnouncement newAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))));

        store.put(nodeId, oldAnnouncement);
        advanceTimeBeyondMaxAge();
        store.put(nodeId, newAnnouncement);

        assertEqualsIgnoreOrder(store.getAll(), newAnnouncement.getServiceAnnouncements().stream()
                .map(toServiceWith(nodeId, newAnnouncement.getLocation(), newAnnouncement.getPool()))
                .collect(toImmutableList()));
    }

    @Test
    public void testPutMultipleForDifferentNodes()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("http", "http://localhost:2222"))));

        Id<Node> greenNodeId = Id.random();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))));

        store.put(blueNodeId, blue);
        store.put(redNodeId, red);
        store.put(greenNodeId, green);

        assertEqualsIgnoreOrder(store.getAll(), concat(
                blue.getServiceAnnouncements().stream().map(toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                red.getServiceAnnouncements().stream().map(toServiceWith(redNodeId, red.getLocation(), red.getPool())),
                green.getServiceAnnouncements().stream().map(toServiceWith(greenNodeId, green.getLocation(), green.getPool())))
                .collect(toImmutableList()));
    }

    @Test
    public void testGetByType()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))));

        Id<Node> greenNodeId = Id.random();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))));

        store.put(blueNodeId, blue);
        store.put(redNodeId, red);
        store.put(greenNodeId, green);

        assertEqualsIgnoreOrder(store.get("storage"), concat(
                blue.getServiceAnnouncements().stream().map(toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                red.getServiceAnnouncements().stream().map(toServiceWith(redNodeId, red.getLocation(), red.getPool())))
                .collect(toImmutableList()));

        assertEqualsIgnoreOrder(store.get("monitoring"), green.getServiceAnnouncements().stream()
                .map(toServiceWith(greenNodeId, green.getLocation(), green.getPool()))
                .collect(toImmutableList()));
    }

    @Test
    public void testGetByTypeAndPool()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))));

        Id<Node> greenNodeId = Id.random();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))));

        Id<Node> yellowNodeId = Id.random();
        DynamicAnnouncement yellow = new DynamicAnnouncement("testing", "poolB", "/US/West/SC4/rack1/host1/vm1/slot4", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:4444"))));

        store.put(blueNodeId, blue);
        store.put(redNodeId, red);
        store.put(greenNodeId, green);
        store.put(yellowNodeId, yellow);

        assertEqualsIgnoreOrder(store.get("storage", "poolA"), concat(
                blue.getServiceAnnouncements().stream().map(toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                red.getServiceAnnouncements().stream().map(toServiceWith(redNodeId, red.getLocation(), red.getPool())))
                .collect(toImmutableList()));

        assertEqualsIgnoreOrder(store.get("monitoring", "poolA"),
                green.getServiceAnnouncements().stream().map(toServiceWith(greenNodeId, red.getLocation(), red.getPool()))
                        .collect(toImmutableList()));

        assertEqualsIgnoreOrder(store.get("storage", "poolB"),
                yellow.getServiceAnnouncements().stream().map(toServiceWith(yellowNodeId, red.getLocation(), red.getPool()))
                        .collect(toImmutableList()));
    }

    @Test
    public void testDelete()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111")),
                new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("http", "http://localhost:2222"))));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "monitoring", ImmutableMap.of("http", "http://localhost:2222"))));

        store.put(blueNodeId, blue);
        store.put(redNodeId, red);

        assertEqualsIgnoreOrder(store.getAll(), concat(
                blue.getServiceAnnouncements().stream().map(toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                red.getServiceAnnouncements().stream().map(toServiceWith(redNodeId, red.getLocation(), red.getPool())))
                .collect(toImmutableList()));

        currentTime.increment();

        store.delete(blueNodeId);

        assertEqualsIgnoreOrder(store.getAll(),
                red.getServiceAnnouncements().stream().map(toServiceWith(redNodeId, red.getLocation(), red.getPool()))
                        .collect(toImmutableList()));

        assertTrue(store.get("storage").isEmpty());
        assertTrue(store.get("web", "poolA").isEmpty());
    }

    @Test
    public void testDeleteThenReInsert()
    {
        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "monitoring", ImmutableMap.of("http", "http://localhost:2222"))));

        store.put(redNodeId, red);
        assertEqualsIgnoreOrder(store.getAll(), red.getServiceAnnouncements().stream().map(toServiceWith(redNodeId, red.getLocation(), red.getPool()))
                .collect(toImmutableList()));

        currentTime.increment();

        store.delete(redNodeId);

        currentTime.increment();

        store.put(redNodeId, red);

        assertEqualsIgnoreOrder(store.getAll(),
                red.getServiceAnnouncements().stream()
                        .map(toServiceWith(redNodeId, red.getLocation(), red.getPool()))
                        .collect(toImmutableList()));
    }

    @Test
    public void testCanHandleLotsOfAnnouncements()
    {
        ImmutableSet.Builder<Service> builder = ImmutableSet.builder();
        for (int i = 0; i < 5000; ++i) {
            Id<Node> id = Id.random();
            DynamicServiceAnnouncement serviceAnnouncement = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"));
            DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(serviceAnnouncement));

            store.put(id, announcement);
            builder.add(new Service(serviceAnnouncement.getId(),
                    id,
                    serviceAnnouncement.getType(),
                    announcement.getPool(),
                    announcement.getLocation(),
                    serviceAnnouncement.getProperties()));
        }

        assertEqualsIgnoreOrder(store.getAll(), builder.build());
    }

    private void advanceTimeBeyondMaxAge()
    {
        currentTime.add(new Duration(MAX_AGE.toMillis() * 2, TimeUnit.MILLISECONDS));
    }
}
