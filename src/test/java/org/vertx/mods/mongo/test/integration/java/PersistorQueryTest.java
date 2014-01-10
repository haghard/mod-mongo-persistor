package org.vertx.mods.mongo.test.integration.java;


import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.util.Iterator;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

public class PersistorQueryTest extends TestVerticle {
  public static final String TESTCOLL2 = "testcoll2";
  private EventBus eb;

  static class Patient {
    String id;
    String name;
    int age;
    double temperature;
  }

  static class Patients implements Iterable<Patient> {
    int i = -1;

    final String[] ids;

    final String[] names = {"Joe", "Chuck", "Evan"};

    final int[] ages = {20, 30, 40};

    final double[] temperatures = {36.7, 37.1, 38.1};

    Patients(String[] ids) {
      this.ids = ids;
    }

    @Override
    public Iterator<Patient> iterator() {
      return new Iterator<Patient>() {

        @Override
        public boolean hasNext() {
          return i < ids.length - 1;
        }

        @Override
        public Patient next() {
          i++;
          return new Patient() {{
            this.id = ids[i];
            this.name = names[i];
            this.age = ages[i];
            this.temperature = temperatures[i];
          }};
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  @Override
  public void start() {
    eb = vertx.eventBus();
    JsonObject config = new JsonObject();
    config.putString("address", "test.persistor");
    config.putString("db_name", System.getProperty("vertx.mongo.database", TESTCOLL2));
    config.putString("host", System.getProperty("vertx.mongo.host", "localhost"));
    config.putNumber("port", Integer.valueOf(System.getProperty("vertx.mongo.port", "27017")));
    String username = System.getProperty("vertx.mongo.username");
    String password = System.getProperty("vertx.mongo.password");
    if (username != null) {
      config.putString("username", username);
      config.putString("password", password);
    }
    config.putBoolean("fake", false);
    container.deployModule(System.getProperty("vertx.modulename"), config, 1, new AsyncResultHandler<String>() {
      public void handle(AsyncResult<String> ar) {
        if (ar.succeeded()) {
          PersistorQueryTest.super.start();
        } else {
          ar.cause().printStackTrace();
        }
      }
    });
  }

  @Test
  public void testQuery() throws Exception {
    final String[] ids = {"39d6d76f-8572-4a33-a598-bc7dda1da186",
        "39d6d76f-8572-4a33-a598-bc7dda1da194",
        "39d6d76f-8572-4a33-a598-bc7dda1da195"};

    final JsonObject deleteJson = new JsonObject()
        .putString("collection", TESTCOLL2)
        .putString("action", "delete")
        .putObject("matcher", new JsonObject());

    eb.send("test.persistor", deleteJson, new Handler<Message<JsonObject>>() {
      public void handle(Message<JsonObject> reply) {
        assertEquals("ok", reply.body().getString("status"));

        Patients patients = new Patients(ids);

        for (Patient patient : patients) {
          JsonObject doc = new JsonObject()
              .putString("_id", patient.id)
              .putString("name", patient.name)
              .putNumber("age", patient.age)
              .putNumber("temperature", patient.temperature);

          JsonObject insertJson =
              new JsonObject().putString("collection", TESTCOLL2)
                  .putString("action", "save").putObject("document", doc);

          eb.send("test.persistor", insertJson, new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> reply) {
              assertEquals("ok", reply.body().getString("status"));
              System.out.print("INSERTED");
            }
          });
        }

        //document with complex field
        JsonObject insertJson0 =
            new JsonObject().putString("collection", TESTCOLL2)
                .putString("action", "save").putObject("document", new JsonObject()
                .putString("_id", "wer326324wey5u45u")
                .putObject("person", new JsonObject()
                    .putString("name", "Sam")
                    .putNumber("age", 56)
                    .putNumber("temperature", 35.6f)));

        eb.send("test.persistor", insertJson0, new Handler<Message<JsonObject>>() {
          public void handle(Message<JsonObject> reply) {
            assertEquals("ok", reply.body().getString("status"));
            System.out.print("INSERTED");
          }
        });

      }
    });

    // _id $in { X }
    final JsonObject inQuery = new JsonObject()
        .putString("collection", TESTCOLL2).putString("action", "find")
        .putObject("matcher", new JsonObject()
            .putString("query", " _id $in { \"" + ids[0] + "\" } "));

    eb.send("test.persistor", inQuery, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> reply) {
        assertEquals("ok", reply.body().getString("status"));
        JsonArray resultArray = reply.body().getArray("results");
        assertEquals(1, resultArray.size());
      }
    });

    // _id $in { X } name $eq Joe
    final JsonObject inQuery1 = new JsonObject()
        .putString("collection", TESTCOLL2).putString("action", "find")
        .putObject("matcher", new JsonObject()
            .putString("query", " _id $in { \"" + ids[0] + "\" } name $eq \"Joe\" "));

    eb.send("test.persistor", inQuery1, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> reply) {
        assertEquals("ok", reply.body().getString("status"));
        JsonArray resultArray = reply.body().getArray("results");
        assertEquals(1, resultArray.size());
        testComplete();
      }
    });

    // _id $in { X } name $eq Joe age &gt 80
    final JsonObject inQuery2 = new JsonObject()
        .putString("collection", TESTCOLL2).putString("action", "find")
        .putObject("matcher", new JsonObject()
            .putString("query", " _id $in { \"" + ids[0] + "\" } name $eq \"Joe\" age $gt 80 "));

    eb.send("test.persistor", inQuery2, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> reply) {
        assertEquals("ok", reply.body().getString("status"));
        JsonArray resultArray = reply.body().getArray("results");
        assertEquals(0, resultArray.size());
      }
    });


    // _id $in { X } name $eq Joe age &gt 10 temperature $eq 36.7
    final JsonObject inQuery3 = new JsonObject()
        .putString("collection", TESTCOLL2).putString("action", "find")
        .putObject("matcher", new JsonObject()
            .putString("query", " _id $in { \"" + ids[0] + "\" } name $eq \"Joe\" age $gt 10 temperature $eq 36.7  "));

    eb.send("test.persistor", inQuery3, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> reply) {
        assertEquals("ok", reply.body().getString("status"));
        JsonArray resultArray = reply.body().getArray("results");
        assertEquals(1, resultArray.size());
      }
    });


    final JsonObject inQuery4 = new JsonObject()
        .putString("collection", TESTCOLL2).putString("action", "find")
        .putObject("matcher", new JsonObject()
            .putString("query", " age $lt 40 temperature $lt 37.5  "));

    eb.send("test.persistor", inQuery4, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> reply) {
        assertEquals("ok", reply.body().getString("status"));
        JsonArray resultArray = reply.body().getArray("results");
        assertEquals(2, resultArray.size());
      }
    });

    //  _id $in { X, Y, Z }
    final JsonObject in3Query = new JsonObject()
        .putString("collection", TESTCOLL2).putString("action", "find")
        .putObject("matcher", new JsonObject()
            .putString("query", " _id $in { " + seq(ids) + " } "));

    eb.send("test.persistor", in3Query, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> reply) {
        assertEquals("ok", reply.body().getString("status"));
        JsonArray resultArray = reply.body().getArray("results");
        assertEquals(3, resultArray.size());
      }
    });

    // person.name $eq "Sam"
    final JsonObject cmpQuery = new JsonObject()
        .putString("collection", TESTCOLL2).putString("action", "find")
        .putObject("matcher", new JsonObject()
            .putString("query", " person.name $eq \"Sam\" person.age $lt 60 "));

    eb.send("test.persistor", cmpQuery, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> reply) {
        assertEquals("ok", reply.body().getString("status"));
        JsonArray resultArray = reply.body().getArray("results");
        assertEquals(1, resultArray.size());
        testComplete();
      }
    });

  }

  private static String seq(String[] ids) {
    boolean first = true;
    StringBuilder temp = new StringBuilder();

    for (String s : ids) {
      if (first) {
        temp.append("\"").append(s).append("\"");
        first = false;
      } else {
        temp.append(',').append("\"").append(s).append("\"");
      }
    }
    return temp.toString();
  }
}

