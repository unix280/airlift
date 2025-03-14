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
package com.facebook.airlift.jaxrs;

import com.facebook.airlift.jaxrs.testing.GuavaMultivaluedMap;
import com.facebook.airlift.jaxrs.thrift.ThriftMapper;
import com.facebook.airlift.jaxrs.thrift.ThriftMapperParsingException;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestThriftMapper
{
    @Test
    public void testSuccess()
            throws IOException
    {
        assertRoundTrip(new TestThriftMessage("value1", 1));
        assertRoundTrip(new TestThriftMessage("value2", 2));
        assertRoundTrip(new TestThriftMessage("value3", 3));
    }

    private void assertRoundTrip(TestThriftMessage testThriftMessage)
            throws IOException
    {
        ThriftCodecManager codecManager = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
        ThriftMapper thriftMapper = new ThriftMapper(codecManager);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        MultivaluedMap<String, Object> headers = new GuavaMultivaluedMap<>();
        MediaType mediaType = new MediaType("application", "x-thrift+binary");
        thriftMapper.writeTo(testThriftMessage, TestThriftMessage.class, null, null, mediaType, headers, outputStream);
        TestThriftMessage readFrom = (TestThriftMessage) thriftMapper.readFrom(Object.class, TestThriftMessage.class, null, mediaType, null, new ByteArrayInputStream(outputStream.toByteArray()));
        assertEquals(readFrom.getTestString(), testThriftMessage.getTestString());
        assertEquals(readFrom.getTestLong(), testThriftMessage.getTestLong());
    }

    @Test
    public void testEOFExceptionReturnsThriftMapperParsingException()
            throws IOException
    {
        try {
            ThriftCodecManager codecManager = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
            ThriftMapper thriftMapper = new ThriftMapper(codecManager);
            thriftMapper.readFrom(Object.class, Object.class, null, null, null, new InputStream()
            {
                @Override
                public int read()
                        throws IOException
                {
                    throw new EOFException("forced EOF Exception");
                }

                @Override
                public int read(byte[] b)
                        throws IOException
                {
                    throw new EOFException("forced EOF Exception");
                }

                @Override
                public int read(byte[] b, int off, int len)
                        throws IOException
                {
                    throw new EOFException("forced EOF Exception");
                }
            });
            fail("Should have thrown a ThriftMapperParsingException");
        }
        catch (ThriftMapperParsingException e) {
            assertTrue((e.getMessage()).startsWith("Invalid thrift input for Java type"));
        }
    }

    @Test(expectedExceptions = IOException.class)
    public void testOtherIOExceptionThrowsIOException()
            throws IOException
    {
        try {
            ThriftCodecManager codecManager = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
            ThriftMapper thriftMapper = new ThriftMapper(codecManager);
            thriftMapper.readFrom(Object.class, Object.class, null, null, null, new InputStream()
            {
                @Override
                public int read()
                        throws IOException
                {
                    throw new ZipException("forced ZipException");
                }

                @Override
                public int read(byte[] b)
                        throws IOException
                {
                    throw new ZipException("forced ZipException");
                }

                @Override
                public int read(byte[] b, int off, int len)
                        throws IOException
                {
                    throw new ZipException("forced ZipException");
                }
            });
            fail("Should have thrown an IOException");
        }
        catch (WebApplicationException e) {
            fail("Should not have received a WebApplicationException", e);
        }
    }
}
