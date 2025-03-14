package org.eclipse.jetty.client.transport;

import java.util.Iterator;

public class HttpChannelAccessor
{
    private HttpChannelAccessor() {}

    public static Iterator<HttpChannel> getHttpConnectionChannels(HttpConnection httpConnection)
    {
        return httpConnection.getHttpChannels();
    }
}
