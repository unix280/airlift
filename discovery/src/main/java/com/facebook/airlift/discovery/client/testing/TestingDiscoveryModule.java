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
package com.facebook.airlift.discovery.client.testing;

import com.facebook.airlift.discovery.client.Announcer;
import com.facebook.airlift.discovery.client.DiscoveryAnnouncementClient;
import com.facebook.airlift.discovery.client.DiscoveryLookupClient;
import com.facebook.airlift.discovery.client.MergingServiceSelectorFactory;
import com.facebook.airlift.discovery.client.ServiceAnnouncement;
import com.facebook.airlift.discovery.client.ServiceSelector;
import com.facebook.airlift.discovery.client.ServiceSelectorFactory;
import com.facebook.airlift.discovery.client.ServiceSelectorManager;
import com.facebook.airlift.node.NodeInfo;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import jakarta.inject.Singleton;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class TestingDiscoveryModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        // bind discovery client and dependencies
        binder.bind(InMemoryDiscoveryClient.class).in(Scopes.SINGLETON);
        binder.bind(DiscoveryAnnouncementClient.class).to(Key.get(InMemoryDiscoveryClient.class)).in(Scopes.SINGLETON);
        binder.bind(DiscoveryLookupClient.class).to(Key.get(InMemoryDiscoveryClient.class)).in(Scopes.SINGLETON);

        // bind announcer
        binder.bind(Announcer.class).in(Scopes.SINGLETON);
        // Must create a multibinder for service announcements or construction will fail if no
        // service announcements are bound, which is legal for processes that don't have public services
        newSetBinder(binder, ServiceAnnouncement.class);

        binder.bind(SimpleServiceSelectorFactory.class).in(Scopes.SINGLETON);
        binder.bind(ServiceSelectorFactory.class).to(MergingServiceSelectorFactory.class).in(Scopes.SINGLETON);

        // bind selector manager with initial empty multibinder
        newSetBinder(binder, ServiceSelector.class);
        binder.bind(ServiceSelectorManager.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public MergingServiceSelectorFactory createMergingServiceSelectorFactory(
            SimpleServiceSelectorFactory factory,
            Announcer announcer,
            NodeInfo nodeInfo)
    {
        return new MergingServiceSelectorFactory(factory, announcer, nodeInfo);
    }
}
