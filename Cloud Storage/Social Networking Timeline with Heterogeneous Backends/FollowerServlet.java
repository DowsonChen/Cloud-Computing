package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.PriorityQueue;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;

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


public class FollowerServlet extends HttpServlet {

    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "dowson";
    private static final String URL = "jdbc:mysql://dongsongmysql.cmzyt7fdioxc.us-east-1.rds.amazonaws.com/" + DB_NAME;
    // Database credentials
    private static final String DB_USER = "dowson";
    private static final String DB_PWD = "cds921209";
    // MySQL connection and statement
    private static Connection conn;
    Statement stmt = null;
    
    
    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "172.31.21.125";
    /**
     * The name of your HBase table.
     */
    private static String tableName = "follower";
    /**
     * HTable handler.
     */
    private static HTableInterface relationTable;
    /**
     * HBase connection.
     */
    private static HConnection hconn;
    
    // Save user information
    PriorityQueue<User> queue = new PriorityQueue<User>(); 
    
    public FollowerServlet() {
        // Initialize MySQL
        try {
            // Register JDBC driver
            Class.forName(JDBC_DRIVER);
            // Open a connection
            conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Initialize HBase
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":60000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("HBase not configured!");
            return;
        }
        try {
            hconn = HConnectionManager.createConnection(conf);
            relationTable = hconn.getTable(Bytes.toBytes(tableName));    
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        
        Statement queryStmt = null;
        String id = request.getParameter("id");
        
        Result result = null;
        String list = null;
        try {
            // Read data from HBase
            Get get = new Get(Bytes.toBytes(id));
            result = relationTable.get(get);
            for (KeyValue keyValue : result.raw()) {
                list = new String(keyValue.getValue());
            }
            // Split users
            String[] ids = list.split(",");
            // Read profile from MySQL by id
            for (String userId : ids) {
                String query = "select * from usersinfo where id = " + userId + ";";
                String name = null;
                String profile = null;
                try {
                    queryStmt = conn.createStatement();
                    ResultSet queryResult = queryStmt.executeQuery(query);
                    if (queryResult.next()) {
                        name = queryResult.getString("name");
                        profile = queryResult.getString("profile");
                        User user = new User(name, profile);
                        queue.offer(user);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    if (queryStmt != null) {
                        try {
                            queryStmt.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        // Transfer users in queue to result
        JSONObject jsonResult = new JSONObject();
        JSONArray followers = new JSONArray();
        while (!queue.isEmpty()) {
            JSONObject follower = new JSONObject();
            User temp = queue.poll();
            follower.put("name", temp.name);
            follower.put("profile", temp.profile);
            followers.put(follower);
        }
        jsonResult.put("followers", followers);
        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", jsonResult.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }   
    
    // Create user class
    private class User implements Comparable<User>{
        private String name;
        private String profile;
        public User(String name, String profile) {
            this.name = name;
            this.profile = profile;
        }
        @Override
        public int compareTo(User user) {
            if (this.name.compareTo(user.name) == 0) {
                return this.profile.compareTo(user.profile);
            } else {
                return this.name.compareTo(user.name);
            }
        }
    }
    
}


