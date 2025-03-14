package com.facebook.airlift.http.client.jetty;

import org.eclipse.jetty.io.Content;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class ChunkedBytesContentProvider
        extends AbstractContentProvider
{
    private static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
    private static final int DEFAULT_BUFFER_SIZE = 4096;

    private final byte[] bytes;
    private final int bufferSize;
    private ChunkedIterator iterator;
    String contentType;

    public ChunkedBytesContentProvider(byte[] bytes)
    {
        this(APPLICATION_OCTET_STREAM, bytes, DEFAULT_BUFFER_SIZE);
    }

    public ChunkedBytesContentProvider(byte[] bytes, int bufferSizeInBytes)
    {
        this(APPLICATION_OCTET_STREAM, bytes, bufferSizeInBytes);
    }

    public ChunkedBytesContentProvider(String contentType, byte[] bytes)
    {
        this(contentType, bytes, DEFAULT_BUFFER_SIZE);
    }

    public ChunkedBytesContentProvider(String contentType, byte[] bytes, int bufferSizeInBytes)
    {
        this.bytes = requireNonNull(bytes, "bytes is null");
        checkArgument(bufferSizeInBytes > 0, "bufferSizeInBytes must be greater than zero: %s", bufferSizeInBytes);
        this.bufferSize = bufferSizeInBytes;
        this.iterator = new ChunkedIterator(bytes, this.bufferSize);
        this.contentType = requireNonNull(contentType, "contentType is null");
    }

    @Override
    public String getContentType()
    {
        return contentType;
    }

    @Override
    public long getLength()
    {
        return bytes.length;
    }

    @Override
    public Content.Chunk read()
    {
        if (iterator.hasNext()) {
            // ordering matters
            ByteBuffer buf = iterator.next();
            boolean hasNext = iterator.hasNext();
            return Content.Chunk.from(buf, !hasNext);
        }

        return null;
    }

    @Override
    public void fail(Throwable throwable)
    {
        super.fail(throwable);
        iterator.forEachRemaining(chunk -> {});
    }

    @Override
    public boolean rewind()
    {
        iterator = new ChunkedIterator(bytes, bufferSize);
        return true;
    }

    private static class ChunkedIterator
            implements Iterator<ByteBuffer>
    {
        private final ByteBuffer buffer;
        private final int bufferSize;
        private int index;

        private ChunkedIterator(byte[] bytes, int bufferSize)
        {
            this.buffer = ByteBuffer.wrap(bytes);
            this.bufferSize = bufferSize;
        }

        @Override
        public boolean hasNext()
        {
            return index < buffer.capacity();
        }

        @Override
        public ByteBuffer next()
        {
            if (index == buffer.capacity()) {
                throw new NoSuchElementException();
            }

            int length = min(buffer.capacity() - index, bufferSize);
            buffer.position(index);
            buffer.limit(index + length);
            index += length;
            return buffer;
        }
    }
}
