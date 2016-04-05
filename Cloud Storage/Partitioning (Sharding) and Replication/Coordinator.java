import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.TimeZone;
import java.util.Iterator;
import java.util.Collections;
import java.util.List;
import java.sql.Timestamp;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {
	// Default mode: sharding. Possible string values are "replication" and
	// "sharding"
	private static String storageType = "replication";
	private static boolean valueSet = false;

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances
	 */
	private static final String dataCenter1 = "ec2-52-23-238-8.compute-1.amazonaws.com";
	private static final String dataCenter2 = "ec2-54-173-8-92.compute-1.amazonaws.com";
	private static final String dataCenter3 = "ec2-54-173-5-44.compute-1.amazonaws.com";

	// create a priority queue to store key and corrspoding queue
	// because the block should only be added on the queue of a certain key
	HashMap<String,PriorityQueue<String>> myMap = new HashMap<String,PriorityQueue<String>>();

	@Override
	public void start() {
		// DO NOT MODIFY THIS
		KeyValueLib.dataCenters.put(dataCenter1, 1);
		KeyValueLib.dataCenters.put(dataCenter2, 2);
		KeyValueLib.dataCenters.put(dataCenter3, 3);
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String value = map.get("value");
				// get timestamp for ordering requests
				final String timestamp = new Timestamp(System
						.currentTimeMillis()
						+ TimeZone.getTimeZone("EST").getRawOffset())
						.toString();
				// calculate the hash value of a certain key
				// then get corresponding dns 
				final String dns= getDns(hash(key));

				// if hash map contains the key, then add timestamp to 
				// its queue, if not create a new queue for it
				if (!myMap.containsKey(key)) {
					PriorityQueue<String> pq = new PriorityQueue<String>();
					pq.add(timestamp);
					myMap.put(key, pq);
				} else {
					myMap.get(key).add(timestamp);
				}
				
				// create thread to execute put request, not block
				Thread t = new Thread(new Runnable() {
					public void run() {
						// synchronize on queue of current key
						synchronized (myMap.get(key)) {
							// while loop the determine if to execute
							while (!timestamp.equals(myMap.get(key).peek())) {
									try {
										// if current timestamp not equal the time
										// of the queue head, then current put 
										// request should wait for previouse execution
										myMap.get(key).wait();
									} catch (Exception e) {
										System.out.println("InterruptedException caught");
									}
							}
							// for replication storage, three put requests should be done	
							// for sharding storage, only one put on calculated dns
							if (storageType.equals("replication")) {
								try {
									KeyValueLib.PUT(dataCenter1, key, value);
									KeyValueLib.PUT(dataCenter2, key, value);
									KeyValueLib.PUT(dataCenter3, key, value);
								} catch (IOException e) {
									System.out.println("IOException caught");
								}
							} else {
								try {
									KeyValueLib.PUT(dns, key, value);
								} catch (IOException e) {
									System.out.println("IOException caught");
								}
							}
							// delete timestamp at the head of queue, and notify
							// waiting threads to execute
							myMap.get(key).poll();
							myMap.get(key).notifyAll();
						}
					}
				});
				t.start();
				req.response().end(); // Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String loc = map.get("loc");
				// calculate the hash value of a certain key then
				// get corresponding dns, for replication derectly get the dns
				final String dns;
				if (storageType.equals("replication")) {
					dns = getDns(loc);
				} else {
					if (loc == null || loc.length() == 0) {
						dns = getDns(hash(key));
					} else {
						dns = getDns(loc);
					}
				}
				// use timestamp for ordering requests
				final String timestamp = new Timestamp(System
						.currentTimeMillis()
						+ TimeZone.getTimeZone("EST").getRawOffset())
						.toString();
				
				// if hash map contains the key, then add timestamp to 
				// its queue, if not create a new queue for it
				if (!myMap.containsKey(key)) {
					PriorityQueue<String> pq = new PriorityQueue<String>();
					pq.add(timestamp);
					myMap.put(key, pq);
				} else {
					myMap.get(key).add(timestamp);
				}

				// create thread to execute put request, not block
				Thread t = new Thread(new Runnable() {
					public void run() {
						String value = "";
						// synchronize on queue of current key
						synchronized (myMap.get(key)) {
							// while loop the determine if to execute
							while (true) {
								PriorityQueue<String> pq = myMap.get(key);
								String headStamp = pq.peek();
								if (!timestamp.equals(headStamp)) {
									try {
										pq.wait();
									} catch (Exception e) {
										System.out.println("InterruptedException caught");
									}
								} else {
									break;
								}
							}
							try {
								// if current timestamp not equal the time
								// of the queue head, then current put 
								// request should wait for previouse execution
								value = KeyValueLib.GET(dns, key);
								myMap.get(key).poll();
							} catch (IOException e) {
								System.out.println("IOException caught");
							}
							// notify waiting threads to execute
							myMap.get(key).notifyAll();
						}
						// return "0" if not get the value from key
						if (value == null || value == "") {
							req.response().end("0");
						} else {
							req.response().end(value);
						}
					}
				});
				t.start();
			}
		});

		routeMatcher.get("/storage", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				storageType = map.get("storage");
				// This endpoint will be used by the auto-grader to set the
				// consistency type that your key-value store has to support.
				// You can initialize/re-initialize the required data structures
				// here
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
	
	// a method to return corresponding dns of loc 1, 2, 3
	static String getDns(String loc) {
		if (loc.equals("1")) {
			return dataCenter1;
		} else if (loc.equals("2")) {
			return dataCenter2;
		} else {
			return dataCenter3;
		}
	}
	
	// hash function to decide which data center 
	// to save of a certain key
    static String hash(String key) {
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
        		return "1";
        	case '2':
        	case '4':
        	case '8':
        		return "2";
        	case '3':
        	case '5':
        	case '6':
        	case '9':
        		return "3";
        }
        return "1";
    }
}