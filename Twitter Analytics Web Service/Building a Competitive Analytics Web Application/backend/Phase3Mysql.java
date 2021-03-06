package io.vertx.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.*;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import java.sql.*;

public class HelloWorldVerticle extends AbstractVerticle {

	// MySQL database settings
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_NAME = "tweet_db";
	private static final String URL = "jdbc:mysql://localhost:3306/" + DB_NAME
			+ "?useUnicode=yes&characterEncoding=UTF-8";
	private static final String DB_USER = "root";
	private static final String DB_PWD = "15619";

	private static Connection conn;

	static private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static {
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-4"));
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
				cipherMatrix[i][j] = (char) (temp < 65 ? temp + 26 : temp);
			}
		}
		ArrayList<Character> rst = new ArrayList<Character>();
		int count = 0;
		while (count * 2 < num && count * 2 < num) {
			for (int i = count; i < num - count; i++) {
				rst.add(cipherMatrix[count][i]);
			}
			for (int i = count + 1; i < num - count; i++) {
				rst.add(cipherMatrix[i][num - count - 1]);
			}
			// if only one row /col remains
			if (num - 2 * count == 1 || num - 2 * count == 1) {
				break;
			}
			for (int i = num - count - 2; i >= count; i--) {
				rst.add(cipherMatrix[num - count - 1][i]);
			}
			for (int i = num - count - 2; i >= count + 1; i--) {
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

	private static void initializeConnection() throws ClassNotFoundException, SQLException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
	}

	private static ConcurrentHashMap<Long, AtomicInteger> lockmap = new ConcurrentHashMap<Long, AtomicInteger>();
	private static ConcurrentHashMap<Long, String[]> tweetmap = new ConcurrentHashMap<Long, String[]>();
	private static ConcurrentHashMap<String, Integer> fieldmap = new ConcurrentHashMap<String, Integer>();
	private static final int cacheSize = 200000;

	static {
		fieldmap.put("userid", 0);
		fieldmap.put("username", 1);
		fieldmap.put("timestamp", 2);
		fieldmap.put("text", 3);
		fieldmap.put("hashtag", 4);
		fieldmap.put("ip", 5);
		fieldmap.put("coordinates", 6);
		fieldmap.put("repliedby", 7);
		fieldmap.put("reply_count", 8);
		fieldmap.put("mentioned", 9);
		fieldmap.put("mentioned_count", 10);
		fieldmap.put("favoritedby", 11);
		fieldmap.put("favorite_count", 12);
		fieldmap.put("useragent", 13);
		fieldmap.put("filter_level", 14);
		fieldmap.put("lang", 15);
	}

	static private String[] remote_servers = { "ec2-52-90-26-18.compute-1.amazonaws.com",
			"ec2-54-87-254-62.compute-1.amazonaws.com", "ec2-54-89-61-31.compute-1.amazonaws.com",
			"ec2-54-152-35-237.compute-1.amazonaws.com", "ec2-52-91-237-94.compute-1.amazonaws.com",
			"ec2-52-91-139-36.compute-1.amazonaws.com" };

	HttpClient httpClient;
	
	private static void sqlSet(Statement setStmt, Long tweetid, String[] fields, String[] payload) throws SQLException {
		String syntax = "INSERT INTO q4_table (%s) VALUES (%s)" + "  ON DUPLICATE KEY UPDATE %s";
		StringBuilder query = new StringBuilder();
		query.append("tweetid");
		StringBuilder value = new StringBuilder();
		value.append(tweetid);
		StringBuilder dup = new StringBuilder();

		for (int i = 0; i < fields.length && i < payload.length; i++) {
			query.append(", ").append(fields[i]);
			value.append(", \"").append(payload[i]).append("\"");
			if (i != 0) {
				dup.append(", ");
			}
			dup.append(fields[i]).append("=VALUES(").append(fields[i]).append(")");
		}
		String sqlSet = String.format(syntax, query.toString(), value.toString(), dup.toString());
		
		setStmt.executeUpdate(sqlSet);
	}

	private static String sqlGet(Statement getStmt, Long tweetid, String[] fields) throws SQLException {
		String result = "";
		String syntax = "SELECT %s FROM q4_table where tweetid = " + tweetid;
		StringBuilder query = new StringBuilder();
		for (int i = 0; i < fields.length; i++) {
			if (i != 0) {
				query.append(", ");
			}
			query.append(fields[i]);
		}
		String sqlGet = String.format(syntax, query);
		ResultSet rs = getStmt.executeQuery(sqlGet);
		if (rs.next()) {
			for (int i = 0; i < fields.length; i++) {
				String payload = rs.getString(fields[i]);
				payload = payload.replace(" ", "+");
				result += payload + "\n";
			}
		}
		rs.close();
		return result;
	}


	@Override
	public void start() {
		try {
			initializeConnection();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Router router = Router.router(vertx);
		router.route().handler(BodyHandler.create());
		router.getWithRegex("/q1.*").handler(this::handlePara1);
		router.getWithRegex("/q2.*").handler(this::handlePara2);
		router.getWithRegex("/q3.*").handler(this::handlePara3);
		router.getWithRegex("/q4.*").handler(this::handlePara4);
		router.get("/index").handler(this::handleindex);

		vertx.createHttpServer().requestHandler(router::accept).listen(80);
		httpClient = vertx.createHttpClient();
	}

	private void handleindex(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		response.end("Hello World\n");
	}

	// Q1 request handler
	private void handlePara1(RoutingContext routingContext) {
		HttpServerRequest request = routingContext.request();
		HttpServerResponse response = routingContext.response();
		MultiMap parameters = request.params();
		String key = parameters.get("key");
		String message = parameters.get("message");
		String result = decipher(message, key);
		String re = "OnePiece,691851324638\n" + dateFormat.format(date) + "\n" + result + "\n";
		response.end(re);
	}

	// Q2 request handler
	private void handlePara2(RoutingContext routingContext) {
		HttpServerRequest request = routingContext.request();
		HttpServerResponse response = routingContext.response();
		MultiMap parameters = request.params();
		String uid = parameters.get("userid");
		String hashtag = parameters.get("hashtag");
		Statement stmt = null;
		String result = "";
		// MySQL statement
		try {
			stmt = conn.createStatement();
			String tableName = "tweet_table";
			String sql = String.format("select text from %s where id_tag = \"%s:%s\"", tableName, uid, hashtag);
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

	// Q3 request handler
	private void handlePara3(RoutingContext routingContext) {
		HttpServerRequest request = routingContext.request();
		HttpServerResponse response = routingContext.response();
		MultiMap parameters = request.params();
		String start_date = parameters.get("start_date");
		String end_date = parameters.get("end_date");
		String start_userid = parameters.get("start_userid");
		String end_userid = parameters.get("end_userid");
		String[] words = parameters.get("words").split(",");
		Statement stmt = null;
		int[] word_counts = { 0, 0, 0 };

		try {
			stmt = conn.createStatement();
			String tableName = "word_table";
			String sql = String.format(
					"select words,count from %s FORCE INDEX(id_time) "
							+ "where (userid >= %s and userid <= %s) and (time >= \'%s\' and time <= \'%s\')",
					tableName, start_userid, end_userid, start_date, end_date);
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String[] result_words = rs.getString("words").split(",");
				String[] result_counts = rs.getString("count").split(",");
				// Use binarySearch method to level up speed
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

		String re = "OnePiece,691851324638\n" + words[0] + ":" + word_counts[0] + "\n" + words[1] + ":" + word_counts[1]
				+ "\n" + words[2] + ":" + word_counts[2] + "\n";
		response.end(re);
	}

	// Q4 request handler
	private void handlePara4(RoutingContext routingContext) {
		HttpServerRequest request = routingContext.request();
		final HttpServerResponse response = routingContext.response();
		MultiMap parameters = request.params();
		final long tweetid = Long.parseLong(parameters.get("tweetid"));
		final String op = parameters.get("op");
		final int seq = Integer.parseInt(parameters.get("seq"));
		final String[] fields = parameters.get("fields").split(",");
		final String[] payload = parameters.get("payload").split(",");

		int hash = hashFunc(String.valueOf(tweetid));
		if (hash != 0) {
			String uri = request.uri();
			httpClient.getNow(80, remote_servers[hash], uri, new Handler<HttpClientResponse>() {
				@Override
				public void handle(HttpClientResponse httpClientResponse) {
					httpClientResponse.bodyHandler(new Handler<Buffer>() {
						@Override
						public void handle(Buffer buffer) {
							response.end(buffer.getString(0, buffer.length()));
						}
					});
				}
			});
		} else {
			new Thread(new Runnable() {
				public void run() {
					String re = "OnePiece,691851324638\n";
					lockmap.putIfAbsent(tweetid, new AtomicInteger(1));
					AtomicInteger tweet_seq = lockmap.get(tweetid);

					String result = "";
					Statement stmt = null;

					try {
						synchronized (tweet_seq) {
							stmt = conn.createStatement();
							while (tweet_seq.get() < seq) {
								try {
									tweet_seq.wait();
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
	
							if (op.equals("set")) {
								String[] new_payloads;
								if ((new_payloads = tweetmap.get(tweetid)) != null) {
									for (int i = 0; i < fields.length && i < payload.length; i++) {
										int index = fieldmap.get(fields[i]);
										new_payloads[index] = payload[i];
									}
									tweetmap.put(tweetid, new_payloads);
								} else {
									if (tweetmap.size() < cacheSize) {
										new_payloads = new String[16];
										for (int i = 0; i < fields.length && i < payload.length; i++) {
											int index = fieldmap.get(fields[i]);
											new_payloads[index] = payload[i];
										}
										tweetmap.put(tweetid, new_payloads);
									} else {
										sqlSet(stmt, tweetid, fields, payload);
									}
								}
								re += "success\n";
								response.end(re);
							} else if (op.equals("get")) {
								String[] payloads;
								if ((payloads = tweetmap.get(tweetid)) != null) {
									int index = fieldmap.get(fields[0]);
									if (payloads[index] != null) {
										String req_payload = payloads[index];
										req_payload = req_payload.replace(" ", "+");
										re += req_payload + "\n";
										response.end(re);
									} else {
										re += "\n";
										response.end(re);
									}
								} else {
									result = sqlGet(stmt, tweetid, fields);
									re += result;
									response.end(re);
								}
							}
	
							if (tweet_seq.get() == seq) {
								tweet_seq.incrementAndGet();
							}
							tweet_seq.notifyAll();
						}
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
				}
			}).start();
		}
	}

	private int hashFunc(String input) {
		long val = getNumVal(input, 0, 0);
		return (int) (val % 6);
	}

	private long getNumVal(String str, long val, int index) {
		if (index == str.length() - 1) {
			return val * 7 + (str.charAt(index) - '0');
		}
		long result = val * 7 + (str.charAt(index) - '0');
		return getNumVal(str, result, index + 1);
	}
}
