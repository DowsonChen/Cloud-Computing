package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;

import org.json.JSONObject;
import org.json.JSONArray;


public class RecommendationServlet extends HttpServlet {
    
    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "dowson";
    private static final String URL = "jdbc:mysql://dongsongmysql.cmzyt7fdioxc.us-east-1.rds.amazonaws.com/" + DB_NAME;
    // Database credentials
    private static final String DB_USER = "dowson";
    private static final String DB_PWD = "cds921209";
    private static Connection conn;
    Statement stmt = null;

    // HBase variables
    private static String zkAddr = "172.31.21.125";
    private static String tableName = "followee";
    private static HTableInterface followeeTable;
    private static HConnection hconn;
    
    // Rocord the first tier followee
    HashSet<String> set;
    
    // Record all followee, id and score
    HashMap<String, Integer> map;
    
    // Get first 10 candidates
    PriorityQueue<Candidate> queue;
	
    
	public RecommendationServlet () throws Exception {
	    /**
         * MySQL initialization
         */
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
        /**
         * HBase initialization
         */
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":60000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("HBase not configured!");
            return;
        }
        hconn = HConnectionManager.createConnection(conf);
        followeeTable = hconn.getTable(Bytes.toBytes(tableName)); 
	}

	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
			throws ServletException, IOException {

	    set = new HashSet<String>();
	    map = new HashMap<String, Integer>();
	    queue = new PriorityQueue<Candidate>();
	    
	    
		JSONObject result = new JSONObject();
	    String id = request.getParameter("id");
	    
	    // Get first tier followees
        Result hbaseFirst1 = null;
        String list1 = null;
        try {
            // Read data from HBase
            Get get = new Get(Bytes.toBytes(id));
            hbaseFirst1 = followeeTable.get(get);
            for (KeyValue keyValue : hbaseFirst1.raw()) {
                list1 = new String(keyValue.getValue());
            }
            // First tier id
            set.add(id);
            String[] ids = list1.split(",");
            for (String userId : ids) {
                set.add(userId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        // Second tier followee
        Result hbaseFirst2 = null;
        String list2 = null;
        Iterator iterator = set.iterator();
        while (iterator.hasNext()) {
            String followeeId = (String) iterator.next();
            try {
                // Read data from HBase
                Get get1 = new Get(Bytes.toBytes(followeeId));
                hbaseFirst2 = followeeTable.get(get1);
                for (KeyValue keyValue : hbaseFirst2.raw()) {
                    list2 = new String(keyValue.getValue());
                }
                // Second tier id
                String[] ids = list2.split(",");
                for (String userId : ids) {
                    // Skip first tier id
                    if(!set.contains(userId)) {
                        if (!map.containsKey(userId)) {
                            map.put(userId, 1);
                        } else {
                            int score = map.get(userId) + 1;
                            map.put(userId, score);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        // Loop HashMap, save candidates
        for (String key : map.keySet()) {
            Candidate can = new Candidate(key, map.get(key));
            System.out.println("ID and score are : " + key + "   " +map.get(key));
            queue.offer(can);
        }
        System.out.println("Candidates number are :" + queue.size());
        
        System.out.println("Hightest score and ID  : " + queue.peek().score + " " +  queue.peek().id);
        
        // Query MySQL for name and profile
        JSONArray candidates = new JSONArray();
        int n = 0;
        // Get top 10 and read profile from MySQL
        while (!queue.isEmpty() && n < 10) {
            JSONObject user = new JSONObject();
            Candidate can = queue.poll();
            Statement queryStmt = null;
            String query;
            String name = null;
            String profile = null;
            try {
                query = "select * from usersinfo where id = " + can.id + ";";
                queryStmt = conn.createStatement();
                ResultSet queryResult = queryStmt.executeQuery(query);
                if (queryResult.next()) {
                    name = queryResult.getString("name");
                    profile = queryResult.getString("profile");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            // Add candidate to result
            user.put("name", name);
            user.put("profile", profile);
            candidates.put(user);
            n++;
        }
        
        result.put("recommendation", candidates);
        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();

	}

	@Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        doGet(request, response);
    }
	
	private class Candidate implements Comparable<Candidate>{
	    int score = 0;
	    String id = null;
	    public Candidate (String id, int score) {
	        this.id = id;
	        this.score = score;
	    }
        // Order by score, descending, then by id, acsending
        public int compareTo(Candidate c) {
            if (this.score == c.score) {
                return Integer.parseInt(this.id) - Integer.parseInt(c.id);
            } else {
                return c.score - this.score;
            }
        }
	}
}

