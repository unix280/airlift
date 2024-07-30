package com.facebook.airlift.http.client.thrift;

import com.facebook.airlift.http.client.HeaderName;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;

import javax.annotation.Nullable;

import java.util.List;

public class ThriftResponse<T>
{
    private final int statusCode;
    private final String errorMessage;
    private final ListMultimap<HeaderName, String> headers;
    private final T value;
    private final IllegalArgumentException exception;

    ThriftResponse(
            int statusCode,
            String errorMessage,
            ListMultimap<HeaderName, String> headers,
            T value,
            IllegalArgumentException exception)
    {
        this.statusCode = statusCode;
        this.errorMessage = errorMessage;
        this.headers = headers != null ? ImmutableListMultimap.copyOf(headers) : null;
        this.value = value;
        this.exception = exception;
    }

    public int getStatusCode()
    {
        return statusCode;
    }

    public String getErrorMessage()
    {
        return errorMessage;
    }

    public T getValue()
    {
        return value;
    }

    @Nullable
    public String getHeader(String name)
    {
        List<String> values = getHeaders().get(HeaderName.of(name));
        return values.isEmpty() ? null : values.get(0);
    }

    public List<String> getHeaders(String name)
    {
        return headers.get(HeaderName.of(name));
    }

    public ListMultimap<HeaderName, String> getHeaders()
    {
        return headers;
    }

    public IllegalArgumentException getException()
    {
        return exception;
    }
}
