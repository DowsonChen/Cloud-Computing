import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
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

public class Coordinator extends Verticle {

    // This integer variable tells you what region you are in
    // 1 for US-E, 2 for US-W, 3 for Singapore
    private static int region = KeyValueLib.region;

    // Default mode: Strongly consistent
    // Options: strong, eventual
    private static String consistencyType = "strong";

    // create a priority queue to store key value, based on timestamp
    ConcurrentHashMap<String, PriorityBlockingQueue<Request>> myMap = new ConcurrentHashMap<String, PriorityBlockingQueue<Request>>();

    /**
     * TODO: Set the values of the following variables to the DNS names of your
     * three dataCenter instances. Be sure to match the regions with their DNS!
     * Do the same for the 3 Coordinators as well.
     */
    private static final String dataCenterUSE = "ec2-52-91-175-60.compute-1.amazonaws.com";
    private static final String dataCenterUSW = "ec2-52-87-248-63.compute-1.amazonaws.com";
    private static final String dataCenterSING = "ec2-54-164-6-113.compute-1.amazonaws.com";
    private static final String coordinatorUSE = "ec2-54-85-134-101.compute-1.amazonaws.com";
    private static final String coordinatorUSW = "ec2-52-207-220-143.compute-1.amazonaws.com";
    private static final String coordinatorSING = "ec2-52-207-255-199.compute-1.amazonaws.com";

    @Override
    public void start() {
        KeyValueLib.dataCenters.put(dataCenterUSE, 1);
        KeyValueLib.dataCenters.put(dataCenterUSW, 2);
        KeyValueLib.dataCenters.put(dataCenterSING, 3);
        KeyValueLib.coordinators.put(coordinatorUSE, 1);
        KeyValueLib.coordinators.put(coordinatorUSW, 2);
        KeyValueLib.coordinators.put(coordinatorSING, 3);
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
                final String forwarded = map.get("forward");
                final String forwardedRegion = map.get("region");

                if (consistencyType.equals("strong")) {
                    // If current coordinator is not primary coordinator,
                    // forward
                    if (forwarded == null && hash(key) != region) {
                        String desDNS = getCorDns(hash(key));
                        try {
                            KeyValueLib.FORWARD(desDNS, key, value,
                                    String.valueOf(timestamp));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        // If forward, finish this request
                        // req.response().end();
                    } else {

                        // If hash map contains the key, then add timestamp to
                        // its queue, if not create a new queue for it
                        if (!myMap.containsKey(key)) {
                            PriorityBlockingQueue<Request> pq = new PriorityBlockingQueue<Request>();
                            pq.add(new Request(value, timestamp));
                            myMap.put(key, pq);
                        } else {
                            myMap.get(key).add(new Request(value, timestamp));
                        }

                        // Inform all data centers
                        try {
                            KeyValueLib.AHEAD(key, String.valueOf(timestamp));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        Thread t = new Thread(new Runnable() {
                            public void run() {
                                /*
                                 * TODO: Add code for PUT request handling here
                                 * Each operation is handled in a new thread.
                                 * Use of helper functions is highly recommended
                                 */
                                synchronized (myMap.get(key)) {
                                    while (timestamp != myMap.get(key).peek()
                                            .getTimeStamp()) {
                                        try {
                                            myMap.get(key).wait();
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                    }
                                    // Three independant threads to put data
                                    // into three databases
                                    Thread t1 = new Thread(new putRunnable(
                                            dataCenterUSE, key, value, String
                                                    .valueOf(timestamp),
                                            consistencyType));
                                    t1.start();
                                    Thread t2 = new Thread(new putRunnable(
                                            dataCenterUSW, key, value, String
                                                    .valueOf(timestamp),
                                            consistencyType));
                                    t2.start();
                                    Thread t3 = new Thread(new putRunnable(
                                            dataCenterSING, key, value, String
                                                    .valueOf(timestamp),
                                            consistencyType));
                                    t3.start();
                                    // Join will let current thread wait t1 t2
                                    // t3 finishes first and then continue
                                    try {
                                        t1.join();
                                        t2.join();
                                        t3.join();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    // When all put successfully, inform all
                                    // datacenters this key is free
                                    try {
                                        KeyValueLib.COMPLETE(key,
                                                String.valueOf(timestamp - 1));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    myMap.get(key).poll();
                                    myMap.get(key).notifyAll();
                                }
                            }
                        });
                        t.start();
                    }
                    
                } else {

                    // If current coordinator is not primary coordinator,
                    // forward
                    Thread t = new Thread(new Runnable() {
                        public void run() {
                            if (hash(key) != region) {
                                String desDNS = getCorDns(hash(key));
                                try {
                                    KeyValueLib.FORWARD(desDNS, key, value,
                                            String.valueOf(timestamp));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else {
                                // Three independant threads to put data into
                                // three databases
                                Thread t1 = new Thread(new putRunnable(
                                        dataCenterUSE, key, value, String
                                                .valueOf(timestamp),
                                        consistencyType));
                                t1.start();
                                Thread t2 = new Thread(new putRunnable(
                                        dataCenterUSW, key, value, String
                                                .valueOf(timestamp),
                                        consistencyType));
                                t2.start();
                                Thread t3 = new Thread(new putRunnable(
                                        dataCenterSING, key, value, String
                                                .valueOf(timestamp),
                                        consistencyType));
                                t3.start();
                            }
                        }
                    });
                    t.start();
                }

                req.response().end(); // Do not remove this
            }
        });

        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final Long timestamp = Long.parseLong(map.get("timestamp"));

                if (consistencyType.equals("storng")) {
                    Thread t = new Thread(new Runnable() {
                        public void run() {
                            /*
                             * TODO: Add code for GET requests handling here
                             * Each operation is handled in a new thread. Use of
                             * helper functions is highly recommended
                             */
                            String response = "0";
                            synchronized (myMap.get(key)) {
                                while (timestamp != myMap.get(key).peek()
                                        .getTimeStamp()) {
                                    try {
                                        myMap.get(key).wait();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                                try {
                                    // Currnt region datacenter DNS
                                    response = KeyValueLib.GET(
                                            getDcDns(region), key,
                                            String.valueOf(timestamp),
                                            consistencyType);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                myMap.get(key).poll();
                                myMap.get(key).notifyAll();
                            }
                            if (response == null || response == "") {
                                req.response().end("0");
                            } else {
                                req.response().end(response);
                            }
                        }
                    });
                    t.start();
                } else {
                    // Get request does not update timestamp, no overwriting
                    // problem
                    Thread t = new Thread(new Runnable() {
                        public void run() {
                            String response = "";
                            // Currnt region datacenter DNS
                            try {
                                response = KeyValueLib.GET(getDcDns(region),
                                        key, String.valueOf(timestamp),
                                        consistencyType);
                                req.response().end(response);
                            } catch (Exception e) {
                                req.response().end("");
                            }
                        }
                    });
                    t.start();
                }
            }
        });

        /* This endpoint is used by the grader to change the consistency level */
        routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                consistencyType = map.get("consistency");
                req.response().end();
            }
        });
        routeMatcher.noMatch(new Handler<HttpServerRequest>() {
            @Override
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

        // Order by request timestamp, ascending order
        public int compareTo(Request r) {
            return (int) (this.timestamp - r.getTimeStamp());
        }
    }

    // PUT runnable thread
    class putRunnable implements Runnable {
        private String dataCenterDNS;
        private String key;
        private String value;
        private String timestamp;
        private String consistencyType;

        // Constructer
        public putRunnable(String dataCenterDNS, String key, String value,
                String timestamp, String consistencyType) {
            this.dataCenterDNS = dataCenterDNS;
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
            this.consistencyType = consistencyType;
        }

        public void run() {
            try {
                KeyValueLib.PUT(dataCenterDNS, key, value,
                        String.valueOf(timestamp), consistencyType);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    static String getDcDns(int region) {
        if (region == 1) {
            return dataCenterUSE;
        } else if (region == 2) {
            return dataCenterUSW;
        } else {
            return dataCenterSING;
        }
    }

    // a method to return corresponding dns of east, west and Singapore
    static String getCorDns(int loc) {
        if (loc == 1) {
            return coordinatorUSE;
        } else if (loc == 2) {
            return coordinatorUSW;
        } else {
            return coordinatorSING;
        }
    }

    // hash function to decide which data center
    // to save of a certain key
    static int hash(String key) {
        char[] array = key.toCharArray();
        long sum = 0;
        // add every digit
        for (int i = 0; i < array.length; i++) {
            sum += array[i];
        }
        String str = String.valueOf(sum);
        char ch = str.charAt(str.length() - 1);
        // ASCII : a - 97 , b - 98, c - 99
        // a must be 1, b must be 2, c must be 3
        // so the switch situation as following
        switch (ch) {
        case '0':
        case '1':
        case '7':
            return 1;
        case '2':
        case '4':
        case '8':
            return 2;
        case '3':
        case '5':
        case '6':
        case '9':
            return 3;
        }
        return 1;
    }
}
