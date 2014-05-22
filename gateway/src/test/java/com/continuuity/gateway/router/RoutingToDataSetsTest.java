package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.security.auth.AccessTokenTransformer;
import com.continuuity.security.guice.SecurityModules;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.net.InetAddresses;
import com.google.inject.Guice;
import com.google.inject.Injector;
import junit.framework.Assert;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class RoutingToDataSetsTest {
  private static NettyRouter nettyRouter;
  private static MockHttpService mockService;
  private static int port;

  @BeforeClass
  public static void before() throws Exception {
    Injector injector = Guice.createInjector(new IOModule(), new SecurityModules().getInMemoryModules(),
                                             new DiscoveryRuntimeModule().getInMemoryModules());

    // Starting router
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    AccessTokenTransformer accessTokenTransformer = injector.getInstance(AccessTokenTransformer.class);
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Router.ADDRESS, "localhost");
    port = Networks.getRandomPort();
    cConf.set(Constants.Router.FORWARD, port + ":" + Constants.Service.GATEWAY);
    nettyRouter = new NettyRouter(cConf, InetAddresses.forString("127.0.0.1"),
                                  new RouterServiceLookup(discoveryServiceClient,
                                                          new RouterPathLookup(new NoAuthenticator())),
                                  new SuccessTokenValidator(), accessTokenTransformer, discoveryServiceClient);
    nettyRouter.startAndWait();

    // Starting mock DataSet service
    DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);
    mockService = new MockHttpService(discoveryService, Constants.Service.DATASET_MANAGER,
                                      new MockDatasetTypeHandler(), new DatasetInstanceHandler());
    mockService.startAndWait();
  }

  @AfterClass
  public static void after() {
    try {
      nettyRouter.stopAndWait();
    } finally {
      mockService.stopAndWait();
    }
  }

  @Test
  public void testTypeHandlerRequests() throws Exception {
    Assert.assertEquals("listModules", doRequest("/datasets/modules", "GET"));
    Assert.assertEquals("post:myModule", doRequest("/datasets/modules/myModule", "POST"));
    Assert.assertEquals("delete:myModule", doRequest("/datasets/modules/myModule", "DELETE"));
    Assert.assertEquals("get:myModule", doRequest("/datasets/modules/myModule", "GET"));
    Assert.assertEquals("listTypes", doRequest("/datasets/types", "GET"));
    Assert.assertEquals("getType:myType", doRequest("/datasets/types/myType", "GET"));
  }

  @Test
  public void testInstanceHandlerRequests() throws Exception {
    Assert.assertEquals("list", doRequest("/datasets/instances", "GET"));
    Assert.assertEquals("post:myInstance", doRequest("/datasets/instances/myInstance", "POST"));
    Assert.assertEquals("delete:myInstance", doRequest("/datasets/instances/myInstance", "DELETE"));
    Assert.assertEquals("get:myInstance", doRequest("/datasets/instances/myInstance", "GET"));
  }

  @Path("/" + Constants.Gateway.GATEWAY_VERSION_NEXT)
  public static final class MockDatasetTypeHandler extends AbstractHttpHandler {
    @GET
    @Path("/datasets/modules")
    public void listModules(HttpRequest request, final HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "listModules");
    }

    @POST
    @Path("/datasets/modules/{name}")
    public void addModule(HttpRequest request, final HttpResponder responder,
                          @PathParam("name") String name) throws IOException {
      responder.sendString(HttpResponseStatus.OK, "post:" + name);
    }

    @DELETE
    @Path("/datasets/modules/{name}")
    public void deleteModule(HttpRequest request, final HttpResponder responder, @PathParam("name") String name) {
      responder.sendString(HttpResponseStatus.OK, "delete:" + name);
    }

    @GET
    @Path("/datasets/modules/{name}")
    public void getModuleInfo(HttpRequest request, final HttpResponder responder, @PathParam("name") String name) {
      responder.sendString(HttpResponseStatus.OK, "get:" + name);
    }

    @GET
    @Path("/datasets/types")
    public void listTypes(HttpRequest request, final HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "listTypes");
    }

    @GET
    @Path("/datasets/types/{name}")
    public void getTypeInfo(HttpRequest request, final HttpResponder responder,
                            @PathParam("name") String name) {
      responder.sendString(HttpResponseStatus.OK, "getType:" + name);
    }
  }

  @Path("/" + Constants.Gateway.GATEWAY_VERSION_NEXT)
  public static final class DatasetInstanceHandler extends AbstractHttpHandler {
    @GET
    @Path("/datasets/instances/")
    public void list(HttpRequest request, final HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "list");
    }

    @GET
    @Path("/datasets/instances/{instance-name}")
    public void getInfo(HttpRequest request, final HttpResponder responder,
                        @PathParam("instance-name") String name) {
      responder.sendString(HttpResponseStatus.OK, "get:" + name);
    }

    @POST
    @Path("/datasets/instances/{instance-name}")
    public void add(HttpRequest request, final HttpResponder responder,
                    @PathParam("instance-name") String name) {
      responder.sendString(HttpResponseStatus.OK, "post:" + name);
    }

    @DELETE
    @Path("/datasets/instances/{instance-name}")
    public void drop(HttpRequest request, final HttpResponder responder,
                     @PathParam("instance-name") String instanceName) {
      responder.sendString(HttpResponseStatus.OK, "delete:" + instanceName);
    }
  }

  private String doRequest(String resource, String requestMethod) throws Exception {
    resource = String.format("http://localhost:%d/%s" + resource, port, Constants.Gateway.GATEWAY_VERSION_NEXT);
    URL url = new URL(resource);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(requestMethod);

    conn.setDoInput(true);

    conn.connect();
    try {
      byte[] responseBody = null;
      if (HttpURLConnection.HTTP_OK == conn.getResponseCode() && conn.getDoInput()) {
        InputStream is = conn.getInputStream();
        try {
          responseBody = ByteStreams.toByteArray(is);
        } finally {
          is.close();
        }
      }
      return new String(responseBody, Charsets.UTF_8);
    } finally {
      conn.disconnect();
    }
  }
}
