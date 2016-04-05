package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.PriorityQueue;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HConnectionManager;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

public class TimelineServlet extends HttpServlet {
    
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
    private static String tableName1 = "follower";
    private static String tableName2 = "followee";
    private static HTableInterface followerTable;
    private static HTableInterface followeeTable;
    private static HConnection hconn;
    // Save follower information
    PriorityQueue<User> queue;
    
    // MongoDB variables
    MongoClient mongoClient;
    private MongoDatabase db;
    // Save followee posts
//    PriorityQueue<Post> postQueue = new PriorityQueue<Post>(); 
    ArrayList<JSONObject> list; 
    
    public TimelineServlet() throws Exception {
        try {
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
            followerTable = hconn.getTable(Bytes.toBytes(tableName1)); 
            followeeTable = hconn.getTable(Bytes.toBytes(tableName2)); 
            /**
             * MongoDB initialzation
             */
            mongoClient = new MongoClient(new ServerAddress("172.31.24.181", 27017));
            db = mongoClient.getDatabase("dowson");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        list = new ArrayList<JSONObject>();
        queue = new PriorityQueue<User>();
        
        JSONObject result = new JSONObject();
        String id = request.getParameter("id");

        /*
            Task 4 (1): Get user profile from MySQL
        */
        String[] idUser = queryMySQL(id);
        String name = idUser[0];
        String profile = idUser[1];
        
        /*
            Task 4 (2): Get follower and followee from HBase
        */
        Result followerResult = null;
        String followerList = null;
        Result followeeResult = null;
        String followeeList = null; 
        try {
            // Read data from HBase
            Get get = new Get(Bytes.toBytes(id));
            followerResult = followerTable.get(get);
            for (KeyValue keyValue : followerResult.raw()) {
                followerList = new String(keyValue.getValue());
            }
            Get get1 = new Get(Bytes.toBytes(id));
            followeeResult = followeeTable.get(get1);
            for (KeyValue keyValue : followeeResult.raw()) {
                followeeList = new String(keyValue.getValue());
            }
            // Split followers
            String[] ids = followerList.split(",");
            // Read follower's profile from MySQL by id
            for (String userId : ids) {
                String[] follower = queryMySQL(userId);
                String followerName = follower[0];
                String follwerProfile = follower[1];
                User user = new User(followerName, follwerProfile);
                queue.offer(user);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        JSONArray followers = new JSONArray();
        while (!queue.isEmpty()) {
            JSONObject follower = new JSONObject();
            User temp = queue.poll();
            follower.put("name", temp.name);
            follower.put("profile", temp.profile);
            followers.put(follower);
        }

        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
       
        /*
            Task 4 (3): Get all followees' posts from MongoDB
        */
        try {
            // Split followees
            String[] ids = followeeList.split(",");
            System.out.println(" Followee ids are : " + Arrays.toString(ids));
            // Ger post from MongoDB and save in postQueue
            for (String userId : ids) {
                
                FindIterable<Document> iterable = db.getCollection("post").find(
                        new Document("uid", Integer.parseInt(userId)));
                
                iterable.forEach(new Block<Document>() {
                    public void apply(final Document document) {
                        JSONObject jb = new JSONObject(document);
                        list.add(jb);
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("All post number is : " + list.size());
        System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
        // Get latest 30
        Collections.sort(list, new Comparator1());
        System.out.println("First 10 posts are:");
        for (int i = 0; i < 30; i++) {
            JSONObject jb = list.get(i);
            String time = jb.getString("timestamp");
            System.out.println(time);
        }
        
        
        
        ArrayList<JSONObject> thirty = new ArrayList<JSONObject>();
        int n = 0;
        // Return 30 posts at most
        for (int i = 0; i < list.size() && n < 30; i++, n++) {
            thirty.add(list.get(i));
        }
        System.out.println("Thirty post arraylist size is : " + thirty.size());
        // Sort 30 posts
        Collections.sort(thirty, new Comparator2());
        
        System.out.println("30 posts are:");
        for (int i = 0; i < 30; i++) {
            JSONObject jb = thirty.get(i);
            String time = jb.getString("timestamp");
            System.out.println(time);
        }
       
        JSONArray posts = new JSONArray();
        for (int i = 0; i < thirty.size(); i++) {
            JSONObject ob = new JSONObject();
            ob = thirty.get(i);
            posts.put(ob);
        }
        
//        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        // Put everything in result JSON
        result.put("followers", followers);
        result.put("name", name);
        result.put("posts", posts);
        result.put("profile", profile);
        PrintWriter out = response.getWriter();
        out.print(String.format("returnRes(%s)", result.toString()));
        out.close();
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
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
    
    public class Comparator1 implements Comparator<JSONObject> {
        @Override
        public int compare(JSONObject o1, JSONObject o2) {
            try {
                String time1 = o1.getString("timestamp");
                String time2 = o2.getString("timestamp");
                SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date1 = dateFmt.parse(time1);
                Date date2 = dateFmt.parse(time2);
                return date2.compareTo(date1);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return 0;
        }
    }
    
    public class Comparator2 implements Comparator<JSONObject> {
        @Override
        public int compare(JSONObject o1, JSONObject o2) {
            try {
                String time1 = o1.getString("timestamp");
                String time2 = o2.getString("timestamp");
                int pid1 = o1.getInt("pid");
                int pid2 = o2.getInt("pid");
                SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date1 = dateFmt.parse(time1);
                Date date2 = dateFmt.parse(time2);
                if (date1.equals(date2)) {
                    return pid1- pid2;
                } else {
                    return date1.compareTo(date2);    
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return 0;
        }
    }
    
//    // Post class
//    private class Post implements Comparable<Post> {
//        String timpstamp;
//        int pid;
//        JSONObject jb;
//        public Post(String timestamp, int pid, JSONObject jb) {
//            this.timpstamp = timestamp;
//            this.pid = pid;
//            this.jb = jb;
//        }
//        private final SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        // Order by timestamp
//        public int compareTo(Post p) {
//            Date start =  null;
//            Date end = null;
//            try {
//                start = dateFmt.parse(this.timpstamp);
//                end = dateFmt.parse(p.timpstamp);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            return end.compareTo(start);
//        }
//    }
//    // Post class
//    private class resultPost implements Comparable<resultPost> {
//        String timpstamp;
//        int pid;
//        JSONObject jb;
//        public resultPost(String timestamp, int pid, JSONObject jb) {
//            this.timpstamp = timestamp;
//            this.pid = pid;
//            this.jb = jb;
//        }
//        private final SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        // Order by timestamp
//        public int compareTo(resultPost p) {
//            Date start =  null;
//            Date end = null;
//            try {
//                start = dateFmt.parse(this.timpstamp);
//                end = dateFmt.parse(p.timpstamp);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            // Descending order
//            if (end.compareTo(start) == 0) {
//                return this.pid - p.pid;
//            } else {
//                return start.compareTo(end);
//            }
//        }
//    }
    
    // Query MySQL method
    private String[] queryMySQL(String id) {
        String query = "select * from usersinfo where id = " + id + ";";
        String name = null;
        String profile = null;
        Statement queryStmt = null;
        String[] info = new String[2];
        try {
            queryStmt = conn.createStatement();
            ResultSet queryResult = queryStmt.executeQuery(query);
            if (queryResult.next()) {
                name = queryResult.getString("name");
                profile = queryResult.getString("profile");
                info[0] = name;
                info[1] = profile;
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
        return info;
    }
}

