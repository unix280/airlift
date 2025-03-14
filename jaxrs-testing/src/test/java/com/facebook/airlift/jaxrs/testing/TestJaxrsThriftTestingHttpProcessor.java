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
package com.facebook.airlift.jaxrs.testing;

import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.http.client.thrift.ThriftBodyGenerator;
import com.facebook.airlift.http.client.thrift.ThriftRequestUtils;
import com.facebook.airlift.http.client.thrift.ThriftResponse;
import com.facebook.airlift.http.client.thrift.ThriftResponseHandler;
import com.facebook.airlift.jaxrs.ParsingExceptionMapper;
import com.facebook.airlift.jaxrs.thrift.ThriftMapper;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.google.common.net.HttpHeaders;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;

import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_BINARY;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_COMPACT;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_FB_COMPACT;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.prepareThriftDelete;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.prepareThriftGet;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.prepareThriftPost;
import static com.facebook.drift.protocol.TType.STOP;
import static com.facebook.drift.transport.netty.codec.Protocol.BINARY;
import static com.facebook.drift.transport.netty.codec.Protocol.COMPACT;
import static com.facebook.drift.transport.netty.codec.Protocol.FB_COMPACT;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test
public class TestJaxrsThriftTestingHttpProcessor
{
    private ThriftCodecManager codecManager;
    private TestingHttpClient httpClient;
    private ThriftCodec<TestThriftMessage> testThriftMessageThriftCodec;
    private ThriftResponseHandler<TestThriftMessage> testThriftMessageTestThriftResponseHandler;

    @BeforeClass
    public void setup()
    {
        codecManager = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
        httpClient =
                new TestingHttpClient(
                        new JaxrsTestingHttpProcessor(
                                URI.create("http://fake.invalid/"),
                                new Resource(),
                                new ThriftMapper(codecManager),
                                new ParsingExceptionMapper()));
        testThriftMessageThriftCodec = codecManager.getCodec(TestThriftMessage.class);
        testThriftMessageTestThriftResponseHandler = new ThriftResponseHandler<>(testThriftMessageThriftCodec);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        codecManager = null;
        httpClient = null;
        testThriftMessageThriftCodec = null;
        testThriftMessageTestThriftResponseHandler = null;
    }

    @DataProvider
    public Object[][] protocolCombinations()
    {
        return new Object[][] {
                {COMPACT},
                {BINARY},
                {FB_COMPACT}
        };
    }

    @Test(dataProvider = "protocolCombinations")
    public void testPost(Protocol protocol)
    {
        Request request = prepareThriftPost(protocol, new TestThriftMessage("xyz", 1), testThriftMessageThriftCodec)
                .setUri(URI.create("http://fake.invalid/http-thrift/2"))
                .build();

        ThriftResponse<TestThriftMessage> response = httpClient.execute(request, testThriftMessageTestThriftResponseHandler);

        assertEquals(response.getStatusCode(), HttpStatus.OK.code());
        assertNotNull(response.getValue());
        assertEquals(response.getValue().getTestString(), "xyz");
        assertEquals(response.getValue().getTestLong(), 3);
    }

    @Test(dataProvider = "protocolCombinations")
    public void testGet(Protocol protocol)
    {
        Request request = prepareThriftGet(protocol)
                .setUri(URI.create("http://fake.invalid/http-thrift/2"))
                .build();

        ThriftResponse<TestThriftMessage> response = httpClient.execute(request, testThriftMessageTestThriftResponseHandler);

        assertEquals(response.getStatusCode(), HttpStatus.OK.code());
        assertNotNull(response.getValue());
        assertEquals(response.getValue().getTestString(), "abc");
        assertEquals(response.getValue().getTestLong(), 2);
    }

    @Test(dataProvider = "protocolCombinations")
    public void testDelete(Protocol protocol)
    {
        Request request = prepareThriftDelete(protocol)
                .setUri(URI.create("http://fake.invalid/http-thrift/2"))
                .build();

        ThriftResponse<TestThriftMessage> response = httpClient.execute(request, testThriftMessageTestThriftResponseHandler);

        assertEquals(response.getStatusCode(), HttpStatus.OK.code());
    }

    @Test
    public void testInvalidFormat()
    {
        Request request = preparePost()
                .setHeader(ACCEPT, "application/x-thrift; t=nonbinary")
                .setHeader(CONTENT_TYPE, "application/x-thrift")
                .setBodyGenerator(createThriftBodyGenerator(COMPACT))
                .setUri(URI.create("http://fake.invalid/http-thrift/2"))
                .build();
        ThriftResponse response = httpClient.execute(request, testThriftMessageTestThriftResponseHandler);
        assertEquals(response.getStatusCode(), HttpStatus.UNSUPPORTED_MEDIA_TYPE.code());
    }

    @Test
    public void testInvalidRequestBody()
    {
        Request request = preparePost()
                .setHeader(ACCEPT, ThriftRequestUtils.APPLICATION_THRIFT_COMPACT)
                .setHeader(HttpHeaders.CONTENT_TYPE, ThriftRequestUtils.APPLICATION_THRIFT_COMPACT)
                //Setting an invalid request body
                .setBodyGenerator(out -> out.write(new byte[] {'C', 'A', 'F', 'E', 'B', 'A', 'B', 'E', STOP}))
                .setUri(URI.create("http://fake.invalid/http-thrift/2"))
                .build();

        ThriftResponse<TestThriftMessage> response = httpClient.execute(request, testThriftMessageTestThriftResponseHandler);
        assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST.code());
        assertTrue(response.getErrorMessage().startsWith("com.facebook.airlift.jaxrs.thrift.ThriftMapperParsingException"));
    }

    private ThriftBodyGenerator<TestThriftMessage> createThriftBodyGenerator(Protocol protocol)
    {
        return new ThriftBodyGenerator<>(new TestThriftMessage("xyz", 1), testThriftMessageThriftCodec, protocol);
    }

    @Test
    public void testException()
    {
        Request request = prepareGet()
                .setUri(URI.create("http://fake.invalid/http-thrift/fail/testException"))
                .build();

        try {
            httpClient.execute(request, testThriftMessageTestThriftResponseHandler);
            fail("expected exception");
        }
        catch (TestingException e) {
            assertEquals(e.getMessage(), "testException");
        }
    }

    @Test
    public void testUndefinedResource()
    {
        Request request = prepareGet()
                .setUri(URI.create("http://fake.invalid/unknown"))
                .build();

        ThriftResponse<TestThriftMessage> response = httpClient.execute(request, testThriftMessageTestThriftResponseHandler);
        assertEquals(response.getStatusCode(), 404);
    }

    @Test
    public void testServerErrorResponse()
    {
        String errorMessage = "test";
        Request request = prepareGet()
                .setHeader(ACCEPT, ThriftRequestUtils.APPLICATION_THRIFT_COMPACT)
                .setHeader(HttpHeaders.CONTENT_TYPE, ThriftRequestUtils.APPLICATION_THRIFT_COMPACT)
                .setUri(URI.create("http://fake.invalid/http-thrift/server-error/" + errorMessage))
                .build();
        try {
            ThriftResponse<TestThriftMessage> response = httpClient.execute(request, testThriftMessageTestThriftResponseHandler);
            assertEquals(response.getErrorMessage(), errorMessage);
            assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR.code());
        }
        catch (TestingException e) {
            fail("exception is not expected");
        }
    }

    @Path("http-thrift")
    public static class Resource
    {
        @Path("{id}")
        @GET
        @Produces({APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
        public TestThriftMessage getTestMessage(@PathParam("id") long id)
        {
            return new TestThriftMessage("abc", id);
        }

        @Path("{id}")
        @POST
        @Consumes({APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
        @Produces({APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
        public TestThriftMessage postTestMessage(@PathParam("id") long id, TestThriftMessage testThriftMessage)
        {
            return new TestThriftMessage(testThriftMessage.getTestString(), id + testThriftMessage.getTestLong());
        }

        @Path("{id}")
        @DELETE
        @Produces({APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
        public Response deleteTestMessage(@PathParam("id") long id)
        {
            return Response.status(Response.Status.OK).build();
        }

        @Path("fail/{message}")
        @GET
        @Consumes({APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
        @Produces({APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
        public TestThriftMessage fail(@PathParam("message") String errorMessage)
        {
            throw new TestingException(errorMessage);
        }

        @Path("server-error/{message}")
        @GET
        @Consumes({APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
        @Produces({APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
        public Response serverErrorResponse(@PathParam("message") String errorMessage)
        {
            return Response.serverError().entity(errorMessage).build();
        }
    }

    private static class TestingException
            extends RuntimeException
    {
        public TestingException(String message)
        {
            super(message);
        }
    }
}
