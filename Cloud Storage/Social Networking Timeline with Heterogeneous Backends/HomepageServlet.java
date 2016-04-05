package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.PriorityQueue;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import org.bson.Document;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;


public class HomepageServlet extends HttpServlet {
    
    MongoClient mongoClient;
    private MongoDatabase db;
    
    // PriorityQueue to record all posts
    PriorityQueue<Post> queue = new PriorityQueue<Post>();

    public HomepageServlet() {
        try {
            mongoClient = new MongoClient(new ServerAddress("172.31.24.181", 27017));
            db = mongoClient.getDatabase("dowson");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {

        String id = request.getParameter("id");
        
        // Read from MongoDB
        FindIterable<Document> iterable = db.getCollection("post").find(
                new Document("uid", Integer.parseInt(id)));
        
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                JSONObject jb = new JSONObject(document);
                String timestamp = jb.getString("timestamp");
                // Save as post object
                Post post = new Post(timestamp, jb);
                queue.offer(post);
            }
        });
        
        JSONObject result = new JSONObject();
        JSONArray allPost = new JSONArray();
        while (!queue.isEmpty()) {
            JSONObject post = new JSONObject();
            Post temp = queue.poll();
            post = temp.jb;
            allPost.put(post);
        }
        result.put("posts", allPost);
        PrintWriter writer = response.getWriter();           
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }
    
    private class Post implements Comparable<Post> {
        String timpstamp;
        JSONObject jb;
        public Post( String timestamp, JSONObject jb) {
            this.timpstamp = timestamp;
            this.jb = jb;
        }
        private final SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // Order by timestamp
        public int compareTo(Post p) {
            Date start =  null;
            Date end = null;
            try {
                start = dateFmt.parse(this.timpstamp);
                end = dateFmt.parse(p.timpstamp);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (start.before(end)) {
               return -1; 
            } else {
               return 1;
            }
        }
    }
}

