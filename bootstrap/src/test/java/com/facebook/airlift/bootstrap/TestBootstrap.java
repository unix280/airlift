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
package com.facebook.airlift.bootstrap;

import com.facebook.airlift.configuration.Config;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.ConfigurationException;
import com.google.inject.CreationException;
import com.google.inject.Module;
import com.google.inject.ProvisionException;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.testing.Assertions.assertContains;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestBootstrap
{
    @Test
    public void testRequiresExplicitBindings()
            throws Exception
    {
        Bootstrap bootstrap = new Bootstrap();
        try {
            bootstrap.initialize().getInstance(Instance.class);
            fail("should require explicit bindings");
        }
        catch (ConfigurationException e) {
            assertContains(e.getErrorMessages().iterator().next().getMessage(), "Explicit bindings are required");
        }
    }

    @Test
    public void testDoesNotAllowCircularDependencies()
            throws Exception
    {
        Bootstrap bootstrap = new Bootstrap(binder -> {
            binder.bind(InstanceA.class);
            binder.bind(InstanceB.class);
        });

        try {
            bootstrap.initialize().getInstance(InstanceA.class);
            fail("should not allow circular dependencies");
        }
        catch (ProvisionException e) {
            assertContains(e.getErrorMessages().iterator().next().getMessage(), "circular dependencies are disabled");
        }
    }

    @Test(expectedExceptions = CreationException.class, expectedExceptionsMessageRegExp = ".*Configuration property 'not-supported' was not used.*")
    public void testStrictConfigDefault()
    {
        new Bootstrap(new ConfigModule())
                .setRequiredConfigurationProperties(ImmutableMap.of("not-supported", "1"))
                .initialize();
    }

    @Test(expectedExceptions = CreationException.class, expectedExceptionsMessageRegExp = ".*Configuration property 'not-supported' was not used.*")
    public void testStrictConfig()
    {
        System.setProperty("bootstrap.strict-config", "true");
        new Bootstrap(new ConfigModule())
                .setRequiredConfigurationProperties(ImmutableMap.of("not-supported", "1"))
                .initialize();
    }

    @Test
    public void testNoStrictConfig()
    {
        System.setProperty("bootstrap.strict-config", "false");
        new Bootstrap(new ConfigModule())
                .setRequiredConfigurationProperties(ImmutableMap.of("not-supported", "1"))
                .initialize()
                .getInstance(LifeCycleManager.class)
                .stop();
    }

    @Test
    public void testEnvironmentVariableReplacement()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                // Literal value
                .put("no-change", "no-change")
                // Correct values with defined environment variables
                .put("variable", "${ENV:VARIABLE}")
                .put("mixed-variable", "mixed-${ENV:VARIABLE}")
                .put("mixed-additional-variable", "mixed-${ENV:ADDITIONAL}-${ENV:VARIABLE}")
                // Broken values with undefined environment variables
                .put("missing-variable", "${ENV:MISSING}")
                .put("mixed-missing-variable", "mixed-${ENV:MISSING}")
                .put("mixed-missing-additional-variable", "mixed-${ENV:MISSING}-${ENV:VARIABLE}")
                .put("mixed-missing-repeated-variable", "mixed-${ENV:MISSING}-${ENV:MISSING}-${ENV:VARIABLE}")
                .put("mixed-missing-many-variables", "mixed-${ENV:MISSING_1}-${ENV:MISSING_2}-${ENV:VARIABLE}")
                .build();
        Map<String, String> environment = new ImmutableMap.Builder<String, String>()
                .put("VARIABLE", "variable")
                .put("ADDITIONAL", "additional")
                .build();
        Map<String, String> errors = new HashMap<>();

        Map<String, String> results = Bootstrap.replaceWithEnvironmentVariables(properties, environment, errors::put);

        assertEquals(results.size(), 4);
        assertEquals(results.get("no-change"), "no-change");
        assertEquals(results.get("variable"), "variable");
        assertEquals(results.get("mixed-variable"), "mixed-variable");
        assertEquals(results.get("mixed-additional-variable"), "mixed-additional-variable");

        assertEquals(errors.size(), 5);
        assertEquals(errors.get("missing-variable"), "Configuration property `missing-variable` references undefined environment variable(s): [MISSING]");
        assertEquals(errors.get("mixed-missing-variable"), "Configuration property `mixed-missing-variable` references undefined environment variable(s): [MISSING]");
        assertEquals(errors.get("mixed-missing-additional-variable"), "Configuration property `mixed-missing-additional-variable` references undefined environment variable(s): [MISSING]");
        assertEquals(errors.get("mixed-missing-repeated-variable"), "Configuration property `mixed-missing-repeated-variable` references undefined environment variable(s): [MISSING]");
        assertEquals(errors.get("mixed-missing-many-variables"), "Configuration property `mixed-missing-many-variables` references undefined environment variable(s): [MISSING_1, MISSING_2]");
    }

    public static class Instance {}

    public static class InstanceA
    {
        @Inject
        public InstanceA(InstanceB b) {}
    }

    public static class InstanceB
    {
        @Inject
        public InstanceB(InstanceA a) {}
    }

    public static class TestingConfig
    {
        private String value;

        public String getValue()
        {
            return value;
        }

        @Config("value")
        public TestingConfig setValue(String value)
        {
            this.value = value;
            return this;
        }
    }

    private static class ConfigModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(TestingConfig.class);
        }
    }
}
