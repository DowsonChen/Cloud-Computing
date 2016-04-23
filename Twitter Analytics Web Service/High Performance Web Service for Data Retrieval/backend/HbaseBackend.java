package io.vertx.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseBackend extends AbstractVerticle {
  private static final String ZOOKEEPERIP = "54.165.253.19";
  private static final String TABLENAME = "twidata";
  private static HConnection conn;
  private static HTableInterface table;
    
  // Connect to HBase and get table
  private static void initializeConnection() throws ClassNotFoundException, 
                                                      ZooKeeperConnectionException, 
                                                      IOException {
    System.out.println("Start connecting...");
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", ZOOKEEPERIP);
    conf.set("hbase.zookeeper.property.clientport", "2181");
    conn = HConnectionManager.createConnection(conf);    
    table = conn.getTable(TABLENAME);
    System.out.println("Finish connecting to " + TABLENAME);
  }

  private String deEscape(String str) {
    long startTime = System.currentTimeMillis();
    String escapedStr = "";
    if (str.length() != 0) {
        escapedStr = str.replace("\\n", "\n")
                        .replace("\\r", "\r")
                        .replace("\\t", "\t");
        escapedStr += "\n";
    }
    long escTime = System.currentTimeMillis() - startTime;
    System.out.println("escape: "+ escTime);
    return escapedStr;
  }

  private void handlePara(RoutingContext routingContext) {
      
    long startTime = System.currentTimeMillis();
    HttpServerRequest request = routingContext.request();
    HttpServerResponse response = routingContext.response();
    MultiMap parameters = request.params();
    String uid = parameters.get("userid");
    String hashtag = parameters.get("hashtag");
    String twidata = "";

    try {
        Get g = new Get(Bytes.toBytes(uid + ":" + hashtag));
        Result result = table.get(g);
        byte [] value = result.getValue(Bytes.toBytes("d"),Bytes.toBytes("t"));
        twidata = Bytes.toString(value);
    } catch (Exception e) {
        e.printStackTrace();
    }
   
    String re = "OnePiece,691851324638\n" + deEscape(twidata) + "\n";
    response.end(re);
  }
  
  
    @Override
    public void start() {
      try {
          initializeConnection();
      } 
      catch (Exception e) {
          e.printStackTrace();
      } 
    
      Router router = Router.router(vertx);
      // Process query 2
      router.getWithRegex("/q2.*").handler(this::handlePara);
    
      vertx.createHttpServer().requestHandler(router::accept).listen(80);
    }
}


