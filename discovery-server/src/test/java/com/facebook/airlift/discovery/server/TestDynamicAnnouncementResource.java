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

import com.facebook.airlift.discovery.store.RealTimeSupplier;
import com.facebook.airlift.jaxrs.testing.MockUriInfo;
import com.facebook.airlift.node.NodeInfo;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.ws.rs.core.Response;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;

import static com.facebook.airlift.discovery.server.DynamicServiceAnnouncement.toServiceWith;
import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestDynamicAnnouncementResource
{
    private InMemoryDynamicStore store;
    private DynamicAnnouncementResource resource;

    @BeforeMethod
    public void setup()
    {
        store = new InMemoryDynamicStore(new DiscoveryConfig(), new RealTimeSupplier());
        resource = new DynamicAnnouncementResource(store, new NodeInfo("testing"));
    }

    @Test
    public void testPutNew()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))));

        Id<Node> nodeId = Id.random();
        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId.toString())), announcement);

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.ACCEPTED.getStatusCode());

        assertEqualsIgnoreOrder(store.getAll(), announcement.getServiceAnnouncements().stream().map(toServiceWith(nodeId, announcement.getLocation(), announcement.getPool())).collect(toImmutableList()));
    }

    @Test
    public void testReplace()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement previous = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "existing"))));

        store.put(nodeId, previous);

        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "new"))));

        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId.toString())), announcement);

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.ACCEPTED.getStatusCode());

        assertEqualsIgnoreOrder(store.getAll(), announcement.getServiceAnnouncements().stream().map(toServiceWith(nodeId, announcement.getLocation(), announcement.getPool())).collect(toImmutableList()));
    }

    @Test
    public void testEnvironmentConflict()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("production", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))));

        Id<Node> nodeId = Id.random();
        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId.toString())), announcement);

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

        assertTrue(store.getAll().isEmpty());
    }

    @Test
    public void testDeleteExisting()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "valueBlue"))));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "valueBlue"))));

        store.put(redNodeId, red);
        store.put(blueNodeId, blue);

        Response response = resource.delete(blueNodeId);

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        assertEqualsIgnoreOrder(store.getAll(), red.getServiceAnnouncements().stream().map(toServiceWith(redNodeId, red.getLocation(), red.getPool())).collect(toImmutableList()));
    }

    @Test
    public void testDeleteMissing()
    {
        Response response = resource.delete(Id.<Node>random());

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        assertTrue(store.getAll().isEmpty());
    }

    @Test
    public void testMakesUpLocation()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "alpha", null, ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))));

        Id<Node> nodeId = Id.valueOf("test123");
        Response response = resource.put(nodeId, new MockUriInfo(URI.create("http://localhost:8080/v1/announcement/" + nodeId.toString())), announcement);

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.ACCEPTED.getStatusCode());

        assertEquals(store.getAll().size(), 1);
        Service service = store.getAll().iterator().next();
        assertEquals(service.getId(), service.getId());
        assertNotNull(service.getLocation());
    }
}
