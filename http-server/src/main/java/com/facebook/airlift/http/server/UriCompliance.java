package com.facebook.airlift.http.server;

public enum UriCompliance
{
    DEFAULT(org.eclipse.jetty.http.UriCompliance.DEFAULT),
    JETTY_11(org.eclipse.jetty.http.UriCompliance.JETTY_11),
    LEGACY(org.eclipse.jetty.http.UriCompliance.LEGACY),
    RFC3986(org.eclipse.jetty.http.UriCompliance.RFC3986),
    UNAMBIGUOUS(org.eclipse.jetty.http.UriCompliance.UNAMBIGUOUS),
    UNSAFE(org.eclipse.jetty.http.UriCompliance.UNSAFE),
    /**/;

    private final org.eclipse.jetty.http.UriCompliance uriCompliance;

    UriCompliance(org.eclipse.jetty.http.UriCompliance uriCompliance)
    {
        this.uriCompliance = uriCompliance;
    }

    public org.eclipse.jetty.http.UriCompliance getUriCompliance()
    {
        return uriCompliance;
    }
}
