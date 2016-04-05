import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.TimeZone;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class KeyValueStore extends Verticle {

    // Store data in datacenter, Key - Value
    private ConcurrentHashMap<String, String> dataCenter = new ConcurrentHashMap<String, String>();

    // Key and timestamp
    ConcurrentHashMap<String, Long> timeStamp = new ConcurrentHashMap<String, Long>();

    // create a priority queue to store key and corrspoding queue
    // because the block should only be added on the queue of a certain key
    ConcurrentHashMap<String, PriorityBlockingQueue<Request>> myMap = new ConcurrentHashMap<String, PriorityBlockingQueue<Request>>();

    private static boolean isLocked;

    @Override
    public void start() {
        final KeyValueStore keyValueStore = new KeyValueStore();
        final RouteMatcher routeMatcher = new RouteMatcher();
        final HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(32767);
        server.setUsePooledBuffers(true);
        server.setReceiveBufferSize(4 * 1024);
        routeMatcher.get("/put", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String value = map.get("value");
                final Long timestamp = Long.parseLong(map.get("timestamp"));
                String consistency = map.get("consistency");
                Integer region = Integer.parseInt(map.get("region"));

                if (consistency.equals("strong")) {
                    // If hash map contains the key, then add timestamp to its
                    // queue, if not create a new queue for it
                    if (!myMap.containsKey(key)) {
                        PriorityBlockingQueue<Request> pq = new PriorityBlockingQueue<Request>();
                        pq.add(new Request(value, timestamp));
                        myMap.put(key, pq);
                    } else {
                        myMap.get(key).add(new Request(value, timestamp));
                    }

                    /*
                     * TODO: Add code here to handle the put request Remember to
                     * use the explicit timestamp if needed!
                     */
                    Thread t = new Thread(new Runnable() {
                        public void run() {
                            // Add clock on queue
                            synchronized (myMap.get(key)) {
                                while (timestamp != myMap.get(key).peek()
                                        .getTimeStamp()) {
                                    try {
                                        myMap.get(key).wait();
                                    } catch (Exception e) {
                                        System.out
                                                .println("InterruptedException caught");
                                    }
                                }
                                try {
                                    // Save data
                                    dataCenter.put(key, value);
                                } catch (Exception e) {
                                    System.out.println("IOException caught");
                                }
                                myMap.get(key).poll();
                                myMap.get(key).notifyAll();
                            }
                        }
                    });
                    t.start();
                } else {
                    if (!timeStamp.containsKey(key)
                            || timestamp > timeStamp.get(key)) {
                        // Update timestamp for key
                        timeStamp.put(key, timestamp);
                        dataCenter.put(key, value);
                    }
                }
                String response = "stored";
                req.response().putHeader("Content-Type", "text/plain");
                req.response().putHeader("Content-Length",
                        String.valueOf(response.length()));
                req.response().end(response);
                req.response().close();
            }
        });

        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final Long timestamp = Long.parseLong(map.get("timestamp"));
                String consistency = map.get("consistency");

                if (consistency.equals("strong")) {
                    // If hash map contains the key, then add timestamp to its
                    // queue, if not create a new queue for it
                    if (!myMap.containsKey(key)) {
                        PriorityBlockingQueue<Request> pq = new PriorityBlockingQueue<Request>();
                        pq.add(new Request("", timestamp));
                        myMap.put(key, pq);
                    } else {
                        myMap.get(key).add(new Request("", timestamp));
                    }

                    Thread t = new Thread(new Runnable() {
                        String response = "";

                        public void run() {
                            synchronized (myMap.get(key)) {
                                // Add lock
                                while (timestamp != myMap.get(key).peek()
                                        .getTimeStamp()) {
                                    try {
                                        myMap.get(key).wait();
                                    } catch (Exception e) {
                                        System.out
                                                .println("InterruptedException caught");
                                    }
                                }
                                try {
                                    // Retrieve data
                                    response = dataCenter.get(key);
                                } catch (Exception e) {
                                    System.out.println("IOException caught");
                                }
                                myMap.get(key).poll();
                                myMap.get(key).notifyAll();
                            }
                            if (response == null || response.equals("")) {
                                response = "0";
                            }
                            req.response().putHeader("Content-Type",
                                    "text/plain");
                            req.response().putHeader("Content-Length",
                                    String.valueOf(response.length()));
                            req.response().end(response);
                            req.response().close();
                        }
                    });
                    t.start();

                } else {

                    String response = "" + dataCenter.get(key);
                    req.response().putHeader("Content-Type", "text/plain");
                    if (response != null) {
                        req.response().putHeader("Content-Length",
                                String.valueOf(response.length()));
                    }
                    req.response().end(response);
                    req.response().close();
                }
            }
        });

        // Clears this stored keys. Do not change this
        routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                dataCenter.clear();
                timeStamp.clear();
                req.response().putHeader("Content-Type", "text/plain");
                req.response().end();
                req.response().close();
            }
        });

        // Handler for when the AHEAD is called
        routeMatcher.get("/ahead", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final Long timestamp = Long.parseLong(map.get("timestamp"));

                /* TODO: Add code to handle the signal here if you wish */
                if (!myMap.containsKey(key)) {
                    PriorityBlockingQueue<Request> pq = new PriorityBlockingQueue<Request>();
                    pq.add(new Request("", timestamp));
                    myMap.put(key, pq);
                } else {
                    myMap.get(key).add(new Request("", timestamp));
                }

                Thread t = new Thread(new Runnable() {
                    public void run() {
                        synchronized (myMap.get(key)) {
                            while (timestamp != myMap.get(key).peek()
                                    .getTimeStamp()) {
                                try {
                                    myMap.get(key).wait();
                                } catch (Exception e) {
                                    System.out
                                            .println("InterruptedException caught");
                                }
                            }
                            myMap.get(key).notifyAll();
                        }
                    }
                });
                t.start();
                req.response().putHeader("Content-Type", "text/plain");
                req.response().end();
                req.response().close();
            }
        });

        // Handler for when the COMPLETE is called
        routeMatcher.get("/complete", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final Long timestamp = Long.parseLong(map.get("timestamp"));
                // See if queue of this key exist
                if (!myMap.containsKey(key)) {
                    PriorityBlockingQueue<Request> pq = new PriorityBlockingQueue<Request>();
                    pq.add(new Request("", timestamp));
                    myMap.put(key, pq);
                } else {
                    myMap.get(key).add(new Request("", timestamp));
                }
                // Make a Thread to unlock
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        synchronized (myMap.get(key)) {
                            while (timestamp != myMap.get(key).peek()
                                    .getTimeStamp()) {
                                try {
                                    myMap.get(key).wait();
                                } catch (Exception e) {
                                    System.out
                                            .println("InterruptedException caught");
                                }
                            }
                            // Poll out this complete request and its folling
                            // lock ahead request
                            myMap.get(key).poll();
                            myMap.get(key).poll();
                            myMap.get(key).notifyAll();
                        }
                    }
                });
                t.start();
                req.response().putHeader("Content-Type", "text/plain");
                req.response().end();
                req.response().close();
            }
        });

        routeMatcher.noMatch(new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest req) {
                req.response().putHeader("Content-Type", "text/html");
                String response = "Not found.";
                req.response().putHeader("Content-Length",
                        String.valueOf(response.length()));
                req.response().end(response);
                req.response().close();
            }
        });
        server.requestHandler(routeMatcher);
        server.listen(8080);
    }

    // Define request class to make request object
    class Request implements Comparable<Request> {
        private long timestamp;
        private String value;

        public Request(String value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        public String getValue() {
            return value;
        }

        public long getTimeStamp() {
            return timestamp;
        }

        // Order by request timestamp
        public int compareTo(Request r) {
            return (int) (this.timestamp - r.getTimeStamp());
        }
    }
}
