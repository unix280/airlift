package com.facebook.airlift.http.client.jetty;

import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.io.Content;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestChunkedBytesContentProvider
{
    @Test
    public void testGetLength()
    {
        Request.Content contentProvider = new ChunkedBytesContentProvider(new byte[] {1, 2, 3});
        assertEquals(contentProvider.getLength(), 3);
        contentProvider = new ChunkedBytesContentProvider(new byte[] {});
        assertEquals(contentProvider.getLength(), 0);
    }

    @Test
    public void testIterator()
    {
        Request.Content contentProvider = new ChunkedBytesContentProvider(new byte[] {}, 2);
        Content.Chunk chunk = contentProvider.read();
        assertNull(chunk);

        contentProvider = new ChunkedBytesContentProvider(new byte[] {1}, 2);
        chunk = contentProvider.read();
        assertTrue(chunk.isLast());
        assertEquals(chunk.getByteBuffer(), ByteBuffer.wrap(new byte[] {1}));
        assertNull(contentProvider.read());

        contentProvider = new ChunkedBytesContentProvider(new byte[] {1, 2}, 2);
        chunk = contentProvider.read();
        assertTrue(chunk.isLast());
        assertEquals(chunk.getByteBuffer(), ByteBuffer.wrap(new byte[] {1, 2}));
        assertNull(contentProvider.read());

        contentProvider = new ChunkedBytesContentProvider(new byte[] {1, 2, 3}, 2);
        chunk = contentProvider.read();
        assertFalse(chunk.isLast());
        assertEquals(chunk.getByteBuffer(), ByteBuffer.wrap(new byte[] {1, 2}));
        chunk = contentProvider.read();
        assertTrue(chunk.isLast());
        assertEquals(chunk.getByteBuffer(), ByteBuffer.wrap(new byte[] {3}));
        assertNull(contentProvider.read());
    }

    @Test
    public void testIsReproducible()
    {
        byte[] bytes = {1, 2, 3};
        Request.Content contentProvider = new ChunkedBytesContentProvider(bytes);
        assertTrue(contentProvider.rewind());
        Content.Chunk chunk = contentProvider.read();
        assertTrue(chunk.isLast());
        assertEquals(chunk.getByteBuffer(), ByteBuffer.wrap(bytes));
        assertNull(contentProvider.read());
        contentProvider.rewind();
        chunk = contentProvider.read();
        assertTrue(chunk.isLast());
        assertEquals(chunk.getByteBuffer(), ByteBuffer.wrap(bytes));
        assertNull(contentProvider.read());
    }
}
