package com.facebook.airlift.jmx;

import org.testng.SkipException;
import org.testng.annotations.Test;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import static org.assertj.core.api.Assertions.assertThat;

public class TestJmxAgent
{
    @Test
    public void testJava9Agent()
            throws Exception
    {
        if (JavaVersion.current().getMajor() < 9) {
            throw new SkipException("Incompatible Java version: " + JavaVersion.current());
        }

        JmxAgent agent = new JmxAgent9(new JmxConfig());

        JMXServiceURL url = agent.getUrl();

        assertThat(url.toString()).matches("service:jmx:rmi:///jndi/rmi://.*:\\d+/jmxrmi");

        JMXConnector connector = JMXConnectorFactory.connect(url);
        connector.connect();
    }
}
