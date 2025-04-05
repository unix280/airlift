/*
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
package com.facebook.airlift.discovery.server.testing;

import com.facebook.airlift.discovery.client.Announcement;
import com.facebook.airlift.discovery.client.DiscoveryAnnouncementClient;
import com.facebook.airlift.discovery.client.DiscoveryLookupClient;
import com.facebook.airlift.discovery.client.HttpDiscoveryAnnouncementClient;
import com.facebook.airlift.discovery.client.HttpDiscoveryLookupClient;
import com.facebook.airlift.discovery.client.ServiceDescriptors;
import com.facebook.airlift.discovery.client.ServiceDescriptorsRepresentation;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.node.NodeInfo;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestTestingDiscoveryServer
{
    private static final String ENVIRONMENT = "testing";
    private static final String SERVICE = "taco";

    @Test(timeOut = 30_000)
    public void testServer()
            throws Exception
    {
        try (TestingDiscoveryServer server = new TestingDiscoveryServer(ENVIRONMENT);
                HttpClient httpClient = new JettyHttpClient()) {
            DiscoveryLookupClient lookup = new HttpDiscoveryLookupClient(
                    server::getBaseUrl,
                    new NodeInfo(ENVIRONMENT),
                    jsonCodec(ServiceDescriptorsRepresentation.class),
                    httpClient);

            DiscoveryAnnouncementClient announcement = new HttpDiscoveryAnnouncementClient(
                    server::getBaseUrl,
                    new NodeInfo(ENVIRONMENT),
                    jsonCodec(Announcement.class),
                    httpClient);

            ServiceDescriptors response = lookup.getServices(SERVICE).get();
            assertEquals(response.getServiceDescriptors().size(), 0);

            announcement.announce(ImmutableSet.of(serviceAnnouncement(SERVICE).build())).get();

            response = lookup.getServices(SERVICE).get();
            assertEquals(response.getServiceDescriptors().size(), 1);

            announcement.unannounce().get();

            response = lookup.getServices(SERVICE).get();
            assertEquals(response.getServiceDescriptors().size(), 0);
        }
    }
}
