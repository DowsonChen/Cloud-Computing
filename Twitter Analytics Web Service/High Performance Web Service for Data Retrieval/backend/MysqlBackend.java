package io.vertx.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.HashMap;
import java.util.Map;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import java.sql.*;

public class MysqlBackend extends AbstractVerticle {
  private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
  private static final String DB_NAME = "tweet_db";
  private static final String URL = "jdbc:mysql://localhost:3306/" + DB_NAME + "?useUnicode=yes&characterEncoding=UTF-8";

  private static final String DB_USER = "root";
  private static final String DB_PWD = "15619";

  private static Connection conn;

  // Connect to Mysql
  private static void initializeConnection() throws ClassNotFoundException, SQLException {
      Class.forName(JDBC_DRIVER);
      conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
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
    // Used for health check
    router.get("/index").handler(this::handleindex);

    vertx.createHttpServer().requestHandler(router::accept).listen(80);
  }

  private void handleindex(RoutingContext routingContext) {
    HttpServerResponse response = routingContext.response();
    response.end("Hello World\n");
  }


  private void handlePara(RoutingContext routingContext) {
    HttpServerRequest request = routingContext.request();
    HttpServerResponse response = routingContext.response();
    MultiMap parameters = request.params();
    String uid = parameters.get("userid");
    String hashtag = parameters.get("hashtag");
    Statement stmt = null;
    String result = "";

    try {
        stmt = conn.createStatement();
        String tableName = "tweet_table";
        String sql = String.format("select text from %s where id_tag = \"%s:%s\"", 
          tableName, uid, hashtag);
        ResultSet rs = stmt.executeQuery(sql);
        if (rs.next()) {
    	    String text = rs.getString("text");
    	    result += text + "\n";
        }
        rs.close();
    } catch (SQLException e) {
        e.printStackTrace();
    } finally {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    
    String re = "OnePiece,691851324638\n" + result + "\n";
    response.end(re);
  }
}


