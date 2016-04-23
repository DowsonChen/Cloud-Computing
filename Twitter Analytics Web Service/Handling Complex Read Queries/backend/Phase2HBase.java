package io.vertx.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.io.IOException;
import java.math.BigInteger;
import java.text.SimpleDateFormat;

//import java.sql.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;

import java.util.*;

public class HelloWorldVerticle extends AbstractVerticle {
	//static SimpleCache<String, String> cache = new SimpleCache<String, String>(200, 500, 20000);
	
	
  private static final String ZOOKEEPERIP = "172.31.2.71";

  private static final String TABLENAME_Q2 = "q2";
  private static final String TABLENAME_Q3 = "q3";
  private static Connection conn;
  
  private static Table tableQ2;
  private static Table tableQ3;
  
  private static byte[] bColFamily = Bytes.toBytes("d");
  private static byte[] bCol1 = Bytes.toBytes("id_time");
  private static byte[] bCol2 = Bytes.toBytes("time");
  private static byte[] bCol3 = Bytes.toBytes("words");
  //private static byte[] bCol3 = Bytes.toBytes("counts");
    
  private static void initializeConnection() throws ClassNotFoundException, 
                                                      ZooKeeperConnectionException, 
                                                      IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", ZOOKEEPERIP);
    conf.set("hbase.zookeeper.property.clientport", "2181");
    conn = ConnectionFactory.createConnection(conf);    
    tableQ3 = conn.getTable(TableName.valueOf(TABLENAME_Q3));
    tableQ2 = conn.getTable(TableName.valueOf(TABLENAME_Q2));
  }
  
  static private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  static {
      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-5"));
  }

  static private Date date = new Date();

  static private long getGCD(String key) {
      // create 3 BigInteger objects
      BigInteger bi1 = new BigInteger(key);
      BigInteger bi2 = new BigInteger(
        "64266330917908644872330635228106713310880186591609208114244758680898150367880703152525200743234420230");
      BigInteger bi3;
      // assign gcd of bi1, bi2 to bi3
      bi3 = bi1.gcd(bi2);
      return bi3.longValue();
  }

  static private String decipher(String message, String key) {
      // get the row and col number
      int len = message.length();
      int num = (int) Math.sqrt(len);
      long z = getGCD(key);
      
      // get intermediate key z
      int keyZ = (int) (1 + z % 25);
      
      // create a 2D matrix of message, and chage to original one
      char[][] cipherMatrix = new char[num][num];
      for (int i = 0; i < num; i++) {
          for (int j = 0; j < num; j++) {
              cipherMatrix[i][j] = message.charAt(i * num + j);
              int temp = cipherMatrix[i][j] - keyZ;
              cipherMatrix[i][j] = (char)( temp < 65 ? temp + 26 : temp);
          }
      }
      
      ArrayList<Character> rst = new ArrayList<Character>();
      
      int count = 0;
      while(count * 2 < num && count * 2 < num) {
          for(int i = count; i < num-count; i++) {
              rst.add(cipherMatrix[count][i]);    
          }
          
          for(int i = count+1; i< num-count; i++) {
              rst.add(cipherMatrix[i][num-count-1]);              
          }

          // if only one row /col remains
          if(num - 2 * count == 1 || num - 2 * count == 1) {
              break;  
          }
              
          for(int i = num-count-2; i>=count; i--) {
              rst.add(cipherMatrix[num-count-1][i]);  
          }
              
          for(int i = num-count-2; i>= count+1; i--) {
              rst.add(cipherMatrix[i][count]);    
          }
          count++;
      }

      StringBuilder builder = new StringBuilder(rst.size());
      for (Character ch : rst) {
          builder.append(ch);
      }
      return builder.toString();
    }
  
  private void handleParaQ1(RoutingContext routingContext) {
	    HttpServerRequest request = routingContext.request();
	    HttpServerResponse response = routingContext.response();
	    MultiMap parameters = request.params();
	    String key = parameters.get("key");
	    String message = parameters.get("message");
	    String result = decipher(message, key);
	    String re = "OnePiece,691851324638\n" + dateFormat.format(date) + "\n" + result + "\n";
	    response.end(re);
	  }
  
  
  private String deEscape(String str) {
	  if (str == null) return null;
      String escapedStr = "";
      if (str.length() != 0) {
          escapedStr = str.replace("\\n", "\n")
                          .replace("\\r", "\r")
                          .replace("\\t", "\t");
          escapedStr += "\n";
      }
      return escapedStr;
  }
  
  private void handleParaQ2(RoutingContext routingContext) {
      
	    long startTime = System.currentTimeMillis();
		  //System.out.println("Request start: " + System.currentTimeMillis());
	    HttpServerRequest request = routingContext.request();
	    HttpServerResponse response = routingContext.response();
	    MultiMap parameters = request.params();
	    String uid = parameters.get("userid");
	    String hashtag = parameters.get("hashtag");
	    String twidata = "";
	    
	/*
	    long paraTime = System.currentTimeMillis() - startTime;
	    System.out.println("get parameters: "+ paraTime);*/
	       
	    try {
	    	
	    	/*String cacheKey = uid + ":" + hashtag;
	        if (cache.get(cacheKey) != null) {
	        	twidata = cache.get(cacheKey);
	        	System.out.println("Hit!");
	        } else {*/
	    	//System.out.println("Get data start: " + System.currentTimeMillis());
	        Get g = new Get(Bytes.toBytes(uid + ":" + hashtag));
	        Result result = tableQ2.get(g);
	        byte [] value = result.getValue(Bytes.toBytes("d"),Bytes.toBytes("t"));
	        twidata = Bytes.toString(value);
	        //System.out.println("Get data end: " + System.currentTimeMillis());
	       /* cache.put(cacheKey, twidata);
	        
	        }*/
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	   
	    String re = "OnePiece,691851324638\n" + deEscape(twidata) + "\n";
	    response.end(re);
	    //System.out.println("Request end: " + System.currentTimeMillis());
	    long reqTime = System.currentTimeMillis() - startTime;
	    if (reqTime >= 10) {
	    	System.out.println("Request time: "+ reqTime);
	    }
	  }

  private void handleParaQ3(RoutingContext routingContext) {
      
    long startTime = System.currentTimeMillis();
	  //System.out.println("Request start: " + System.currentTimeMillis());
    HttpServerRequest request = routingContext.request();
    HttpServerResponse response = routingContext.response();
    MultiMap parameters = request.params();
    String start_date = parameters.get("start_date");
    String end_date = parameters.get("end_date");
    String start_userid = parameters.get("start_userid");
    int int_start = Integer.parseInt(start_userid);
    String end_userid = parameters.get("end_userid");
    int int_end = Integer.parseInt(end_userid);
    String[] words = parameters.get("words").split(",");
    int[] word_counts = {0, 0, 0};
       
    try {
        Scan scan = new Scan();
        scan.addColumn(bColFamily, bCol1);
        scan.addColumn(bColFamily, bCol2);
        scan.addColumn(bColFamily, bCol3);
        String startRow = start_userid + ":" + start_date;
        String endRow = end_userid + ":" + end_date;
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));
        BinaryComparator comp1 = new BinaryComparator(Bytes.toBytes(start_date));
        Filter filter1 = new SingleColumnValueFilter(bColFamily, bCol1, CompareFilter.CompareOp.GREATER_OR_EQUAL, comp1);
        BinaryComparator comp2 = new BinaryComparator(Bytes.toBytes(end_date));
        Filter filter2 = new SingleColumnValueFilter(bColFamily, bCol1, CompareFilter.CompareOp.LESS_OR_EQUAL, comp2);
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(filter1);
        list.addFilter(filter2);
        scan.setFilter(list);

        ResultScanner rs = tableQ3.getScanner(scan);

        for (Result r = rs.next(); r != null; r = rs.next()) {
          String[] id_time = Bytes.toString(r.getRow()).split(":");
          int user_id = Integer.parseInt(id_time[0]);
          if (user_id < int_start || user_id > int_end)
            continue;
          String[] result_words = Bytes.toString(r.getValue(bColFamily, bCol2)).split(",");
          String[] result_counts = Bytes.toString(r.getValue(bColFamily, bCol3)).split(",");
          int index0 = Arrays.binarySearch(result_words, words[0]);
          int index1 = Arrays.binarySearch(result_words, words[1]);
          int index2 = Arrays.binarySearch(result_words, words[2]);
          if (index0 >= 0) {
            int value = Integer.parseInt(result_counts[index0]);
            word_counts[0] += value;
          }
          if (index1 >= 0) {
            int value = Integer.parseInt(result_counts[index1]);
            word_counts[1] += value;
          }
          if (index2 >= 0) {
            int value = Integer.parseInt(result_counts[index2]);
            word_counts[2] += value;
          }
        }
        rs.close();
    } catch (Exception e) {
        e.printStackTrace();
    }
   
    String re = "OnePiece,691851324638\n" + words[0] + ":" + word_counts[0] + "\n" +
                words[1] + ":" + word_counts[1] + "\n" + 
                words[2] + ":" + word_counts[2] + "\n";
    response.end(re);
    //System.out.println("Request end: " + System.currentTimeMillis());
    long reqTime = System.currentTimeMillis() - startTime;
    if (reqTime >= 10) {
    	System.out.println("Request time: "+ reqTime);
    }
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
      router.route().handler(BodyHandler.create());
      router.getWithRegex("/q3.*").handler(this::handleParaQ3);
      router.getWithRegex("/q2.*").handler(this::handleParaQ2);
      router.getWithRegex("/q1.*").handler(this::handleParaQ1);
    
      vertx.createHttpServer().requestHandler(router::accept).listen(80);
    }
}


