package org.olf.marcstorage;

import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class MarcStorageTest {
  static int port;
  private static Vertx vertx;
  private final Logger logger = LoggerFactory.getLogger(MarcStorageTest.class);
  public static String MODULE_TO = "0.0.5";
  public static String MODULE_FROM = "0.0.4";
  static final String marcJsonString1 =
"{" +
"  \"instanceId\":\"478685ba-ed91-4f61-83bd-d1cfe7152753\"," +
"  \"institutionId\":\"b4578dbc-4dd9-4ac1-9c01-8a13f65aa95e\"," +
"  \"localIdentifier\": \"029857716\"," +
"  \"parsedMarc\": {" +
"      \"leader\":\"00452nam a2200169 ca4500\"," +
"      \"fields\":[" +
"        {\"001\":\"029857716\"}," +
"        {\"003\":\"DE-601\"}," +
"        {\"005\":\"20180511131518.0\"}," +
"        {\"008\":\"900626m19799999xxk\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\000\\\\0\\\\eng\\\\d\"}," +
"        {\"035\":{\"ind1\":\"\\\\\"," +
"                \"ind2\":\"\\\\\"," +
"                \"subfields\":[{\"a\":\"(DE-599)GBV029857716\"}]}}," +
"        {\"040\":{\"ind1\":\"\\\\\"," +
"                \"ind2\":\"\\\\\"," +
"                \"subfields\":[{\"b\":\"ger\"}," +
"                             {\"c\":\"GBVCP\"}]}}," +
"        {\"041\":{\"ind1\":\"0\"," +
"                \"ind2\":\"\\\\\"," +
"                \"subfields\":[{\"a\":\"eng\"}]}}," +
"        {\"044\":{\"ind1\":\"\\\\\"," +
"                \"ind2\":\"\\\\\"," +
"                \"subfields\":[{\"a\":\"xxk\"}," +
"                             {\"a\":\"at\"}]}}," +
"        {\"100\":{\"ind1\":\"1\"," +
"                \"ind2\":\"\\\\\"," +
"                \"subfields\":[{\"a\":\"Adams, Douglas\"}]}}," +
"        {\"245\":{\"ind1\":\"1\"," +
"                \"ind2\":\"4\"," +
"                \"subfields\":[{\"a\":\"The Hitch Hiker's guide to the Galaxy\"}," +
"                             {\"c\":\"Douglas Adams\"}]}}," +
"        {\"246\":{\"ind1\":\"1\"," +
"                \"ind2\":\"3\"," +
"                \"subfields\":[{\"i\":\"Nebent.\"}," +
"                             {\"a\":\"The hitchhiker series\"}]}}," +
"        {\"264\":{\"ind1\":\"3\"," +
"                \"ind2\":\"1\"," +
"                \"subfields\":[{\"a\":\"London [u.a.]\"}," +
"                             {\"b\":\"Pan Books\"}," +
"                             {\"c\":\"1979-\"}]}}" +
"      ]" +
"  }" +
"}";

  static final String marcJsonString2 =
"{" +
"  \"instanceId\":\"11dfac11-1caf-4470-9ad1-d533f6360bdd\"," +
"  \"institutionId\":\"b4578dbc-4dd9-4ac1-9c01-8a13f65aa95e\"," +
"  \"localIdentifier\": \"393893\"," +
"  \"parsedMarc\": {" +
"  \"leader\": \"01542ccm a2200361   4500\"," +
"      \"fields\": [" +
"        {" +
"          \"001\": \"393893\"" +
"        }," +
"        {" +
"          \"005\": \"20141107001016.0\"" +
"        }," +
"        {" +
"          \"008\": \"830419m19559999gw mua   hiz   n    lat  \"" +
"        }," +
"        {" +
"          \"010\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"   55001156/M \"" +
"              }" +
"            ]," +
"            \"ind1\": \" \"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"035\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"(OCoLC)63611770\"" +
"              }" +
"            ]," +
"            \"ind1\": \" \"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"035\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"393893\"" +
"              }" +
"            ]," +
"            \"ind1\": \" \"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"040\": {" +
"            \"subfields\": [" +
"              {" +
"                \"c\": \"UPB\"" +
"              }," +
"              {" +
"                \"d\": \"UPB\"" +
"              }," +
"              {" +
"                \"d\": \"NIC\"" +
"              }," +
"              {" +
"                \"d\": \"NIC\"" +
"              }" +
"            ]," +
"            \"ind1\": \" \"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"041\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"latitager\"" +
"              }," +
"              {" +
"                \"g\": \"ger\"" +
"              }" +
"            ]," +
"            \"ind1\": \"0\"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"045\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"v6v9\"" +
"              }" +
"            ]," +
"            \"ind1\": \" \"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"047\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"cn\"" +
"              }," +
"              {" +
"                \"a\": \"ct\"" +
"              }," +
"              {" +
"                \"a\": \"co\"" +
"              }," +
"              {" +
"                \"a\": \"df\"" +
"              }," +
"              {" +
"                \"a\": \"dv\"" +
"              }," +
"              {" +
"                \"a\": \"ft\"" +
"              }," +
"              {" +
"                \"a\": \"fg\"" +
"              }," +
"              {" +
"                \"a\": \"ms\"" +
"              }," +
"              {" +
"                \"a\": \"mi\"" +
"              }," +
"              {" +
"                \"a\": \"nc\"" +
"              }," +
"              {" +
"                \"a\": \"op\"" +
"              }," +
"              {" +
"                \"a\": \"ov\"" +
"              }," +
"              {" +
"                \"a\": \"rq\"" +
"              }," +
"              {" +
"                \"a\": \"sn\"" +
"              }," +
"              {" +
"                \"a\": \"su\"" +
"              }," +
"              {" +
"                \"a\": \"sy\"" +
"              }," +
"              {" +
"                \"a\": \"vr\"" +
"              }," +
"              {" +
"                \"a\": \"zz\"" +
"              }" +
"            ]," +
"            \"ind1\": \" \"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"050\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"M3\"" +
"              }," +
"              {" +
"                \"b\": \".M896\"" +
"              }" +
"            ]," +
"            \"ind1\": \"0\"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"100\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"Mozart, Wolfgang Amadeus,\"" +
"              }," +
"              {" +
"                \"d\": \"1756-1791.\"" +
"              }" +
"            ]," +
"            \"ind1\": \"1\"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"240\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"Works\"" +
"              }" +
"            ]," +
"            \"ind1\": \"1\"," +
"            \"ind2\": \"0\"" +
"          }" +
"        }," +
"        {" +
"          \"245\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"Neue Ausgabe sa\\\\u0308mtlicher Werke, \"" +
"              }," +
"              {" +
"                \"b\": \"in Verbindung mit den Mozartsta\\\\u0308dten, Augsburg, Salzburg und Wien.\"" +
"              }," +
"              {" +
"                \"c\": \"Hrsg. von der Internationalen Stiftung Mozarteum, Salzburg.\"" +
"              }" +
"            ]," +
"            \"ind1\": \"1\"," +
"            \"ind2\": \"0\"" +
"          }" +
"        }," +
"        {" +
"          \"246\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"Neue Mozart-Ausgabe\"" +
"              }" +
"            ]," +
"            \"ind1\": \"3\"," +
"            \"ind2\": \"3\"" +
"          }" +
"        }," +
"        {" +
"          \"260\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"Kassel,\"" +
"              }," +
"              {" +
"                \"b\": \"Ba\\\\u0308renreiter,\"" +
"              }," +
"              {" +
"                \"c\": \"c1955-\"" +
"              }" +
"            ]," +
"            \"ind1\": \" \"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"300\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"v.\"" +
"              }," +
"              {" +
"                \"b\": \"facsims.\"" +
"              }," +
"              {" +
"                \"c\": \"33 cm.\"" +
"              }" +
"            ]," +
"            \"ind1\": \" \"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"505\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"Ser. I. Geistliche Gesangswerke -- Ser. II. Opern -- Ser. III. Lieder, mehrstimmige Gesa\\\\u0308nge, Kanons -- Ser. IV. Orchesterwerke -- Ser. V. Konzerte -- Ser. VI. Kirchensonaten -- Ser. VII. Ensemblemusik fu\\\\u0308r gro\\\\u0308ssere Solobesetzungen -- Ser. VIII. Kammermusik -- Ser. IX. Klaviermusik -- Ser. X. Supplement.\"" +
"              }" +
"            ]," +
"            \"ind1\": \"0\"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"650\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"Vocal music\"" +
"              }" +
"            ]," +
"            \"ind1\": \" \"," +
"            \"ind2\": \"0\"" +
"          }" +
"        }," +
"        {" +
"          \"650\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"Instrumental music\"" +
"              }" +
"            ]," +
"            \"ind1\": \" \"," +
"            \"ind2\": \"0\"" +
"          }" +
"        }," +
"        {" +
"          \"650\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"Instrumental music\"" +
"              }," +
"              {" +
"                \"2\": \"fast\"" +
"              }," +
"              {" +
"                \"0\": \"(OCoLC)fst00974414\"" +
"              }" +
"            ]," +
"            \"ind1\": \" \"," +
"            \"ind2\": \"7\"" +
"          }" +
"        }," +
"        {" +
"          \"650\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"Vocal music\"" +
"              }," +
"              {" +
"                \"2\": \"fast\"" +
"              }," +
"              {" +
"                \"0\": \"(OCoLC)fst01168379\"" +
"              }" +
"            ]," +
"            \"ind1\": \" \"," +
"            \"ind2\": \"7\"" +
"          }" +
"        }," +
"        {" +
"          \"902\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\":\"pfnd\"" +
"              }," +
"              {" +
"                \"b\":\"Austin Music\"" +
"              }" +
"            ]," +
"            \"ind1\":\" \"," +
"            \"ind2\":\" \"" +
"          }" +
"        }," +
"        {" +
"          \"905\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\":\"19980728120000.0\"" +
"              }" +
"            ]," +
"            \"ind1\":\" \"," +
"            \"ind2\":\" \"" +
"          }" +
"        }," +
"        {" +
"          \"948\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"20100622\"" +
"              }," +
"              {" +
"                \"b\": \"s\"" +
"              }," +
"              {" +
"                \"d\": \"lap11\"" +
"              }," +
"              {" +
"                \"e\": \"lts\"" +
"              }," +
"              {" +
"                \"x\": \"ToAddCatStat\"" +
"              }" +
"            ]," +
"            \"ind1\": \"1\"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"948\": {" +
"            \"subfields\": [" +
"              {" +
"                \"a\": \"20110818\"" +
"              }," +
"              {" +
"                \"b\": \"r\"" +
"              }," +
"              {" +
"                \"d\": \"np55\"" +
"              }," +
"              {" +
"                \"e\": \"lts\"" +
"              }" +
"            ]," +
"            \"ind1\": \"0\"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"948\": {" +
"            \"subfields\": [" +
"              {" +
"                 \"a\": \"20130128\"" +
"              }," +
"              {" +
"                 \"b\": \"m\"" +
"              }," +
"              {" +
"                 \"d\": \"bmt1\"" +
"              }," +
"              {" +
"                 \"e\": \"lts\"" +
"              }" +
"            ]," +
"            \"ind1\": \"2\"," +
"            \"ind2\": \" \"" +
"          }" +
"        }," +
"        {" +
"          \"948\": {" +
"            \"subfields\": [" +
"              {" +
"                 \"a\": \"20141106\"" +
"              }," +
"              {" +
"                 \"b\": \"m\"" +
"              }," +
"              {" +
"                 \"d\": \"batch\"" +
"              }," +
"              {" +
"                 \"e\": \"lts\"" +
"              }," +
"              {" +
"                 \"x\": \"addfast\"" +
"              }" +
"            ]," +
"            \"ind1\": \"2\"," +
"            \"ind2\": \" \"" +
"          }" +
"        }" +
"      ]" +
"  }" +
"}";

  String marcRecord1Id;
  String marcRecord2Id;

  @Before
  public void before(TestContext context) {
    Async async = context.async();
 
    JsonObject marcJson1 = new JsonObject(marcJsonString1);
    JsonObject marcJson2 = new JsonObject(marcJsonString2);
    Future<JsonObject> fut1 = postMarcJson(marcJson1);
    Future<JsonObject> fut2 = postMarcJson(marcJson2);

    CompositeFuture compositeFuture = CompositeFuture.all(fut1, fut2);
    compositeFuture.setHandler(res -> {
      if(res.failed()) {
        context.fail(res.cause());
      } else {
        async.complete();
      }
    });
       /*
           .setHandler(res -> {
             JsonObject json = res.result();
             if(json.getInteger("totalResults") < 1) {
               context.fail("Expected non-zero result count");
             } else {
              marcRecord1Id = json.getJsonArray("marcrecords").getJsonObject(0).getString("id");
              async.complete();
             }
           });
       */
       /*
           .compose(json -> postMarcJson(marcJson2)
           .setHandler(res -> {
             JsonObject json2 = res.result();
             marcRecord2Id = json2.getJsonArray("marcrecords").getJsonObject(0).getString("id");
             async.complete();
           })
       );
*/

  }

  @After
  public void after(TestContext context) {
    Async async = context.async();
    deleteAllMarcJson().onComplete(res -> {
      if(res.failed()) {
        context.fail(res.cause());
      } else {
        async.complete();
      }
    });
  }

  @Rule
  public Timeout rule = Timeout.seconds(200);  // 3 minutes for loading embedded postgres

  @BeforeClass
  public static void beforeClass(TestContext context) {
    Async async = context.async();
    port = NetworkUtils.nextFreePort();
    //TenantClient tenantClient = new TenantClient("localhost", port, "diku", "diku");
    vertx = Vertx.vertx();
    DeploymentOptions options = new DeploymentOptions()
        .setConfig(new JsonObject().put("http.port", port));
    try {
      PostgresClient.setIsEmbedded(true);
      PostgresClient.getInstance(vertx).startEmbeddedPostgres();
    } catch(Exception e) {
      e.printStackTrace();
      context.fail(e);
      return;
    }
    vertx.deployVerticle(RestVerticle.class.getName(), options, res -> {
      try {
        initTenant("diku", port).onComplete(initTenantRes -> {
          async.complete();
        });
      } catch(Exception e) {
        e.printStackTrace();
        context.fail(e);
      }
    });
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess( res -> {
      PostgresClient.stopEmbeddedPostgres();
      async.complete();
    }));
  }

  @Test
  public void dummyTest(TestContext context) {
    Async async = context.async();
    async.complete();
  }

  private Future<JsonObject> postMarcJson(JsonObject marcJson) {
    Future<JsonObject> future = Future.future();
    HttpClient client = vertx.createHttpClient();
    HttpClientRequest request;
    Buffer buffer = Buffer.buffer(marcJson.encode());
    String url = String.format("http://localhost:%s/marc-records", port);
    logger.info(String.format("Posting new MARC record to %s", url));
    request = client.postAbs(url);
    request.putHeader("x-okapi-tenant", "diku");
    request.putHeader("Accept", "application/json,text/plain");
    request.putHeader("Content-type", "application/json");
    request.exceptionHandler(e -> { future.fail(e); });
    request.handler(req -> {
      req.bodyHandler( buf -> {
        if(req.statusCode() != 201) {
          future.fail(String.format("Expected status code 201, got %s", req.statusCode()));
          return;
        } else {
          try {
            JsonObject response = buf.toJsonObject();
            future.complete(response);
          } catch(Exception e) {
            future.fail(e);
          }
        }
      });
    });
    request.end(buffer);
    return future;
  }

  private Future<Void> deleteAllMarcJson() {
    Future<Void> future = Future.future();
    logger.info("Deleting all MARC records");
    HttpClient client = vertx.createHttpClient();
    HttpClientRequest request;
    String url = String.format("http://localhost:%s/marc-records", port);
    request = client.deleteAbs(url);
    request.putHeader("x-okapi-tenant", "diku");
    request.putHeader("Accept", "application/json,text/plain");
    request.putHeader("Content-type", "application/json");
    request.exceptionHandler(e -> { future.fail(e); });
    request.handler(req -> {
      req.bodyHandler( buf -> {
        if(req.statusCode() != 204) {
          future.fail(String.format("Expected status code 204, got %s", req.statusCode()));
          return;
        } else {
          future.complete();
        }
      });
    });
    request.end();
    return future;
  }

  private String getIdFromFirstResult(JsonObject resultObject, String index) {
    if(!resultObject.containsKey("totalRecords")) {
      return null;
    }
    Integer totalRecords = resultObject.getInteger("totalRecords");
    if(totalRecords == null || totalRecords < 1) {
      return null;
    }
    if(!resultObject.containsKey("index")) {
      return null;
    }
    JsonObject firstMember = resultObject.getJsonArray(index).getJsonObject(0);
    String id = firstMember.getString("id");
    return id;
  }
  
    private static Future<Void> initTenant(String tenantId, int port) {
    Promise<Void> promise = Promise.promise();
    HttpClient client = vertx.createHttpClient();
    String url = "http://localhost:" + port + "/_/tenant";
    JsonObject payload = new JsonObject()
        .put("module_to", MODULE_TO)
        .put("module_from", MODULE_FROM);
    HttpClientRequest request = client.postAbs(url);
    request.handler(req -> {
      if(req.statusCode() != 201) {
        promise.fail("Expected 201, got " + req.statusCode());
      } else {
        promise.complete();
      }
    });
    request.putHeader("X-Okapi-Tenant", tenantId);
    //request.putHeader("X-Okapi-Url", okapiUrl);
    request.putHeader("Content-Type", "application/json");
    request.putHeader("Accept", "application/json, text/plain");
    request.end(payload.encode());
    return promise.future();
  }

}


