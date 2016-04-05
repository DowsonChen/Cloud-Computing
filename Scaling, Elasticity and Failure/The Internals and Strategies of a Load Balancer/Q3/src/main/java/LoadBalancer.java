import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;

public class LoadBalancer {
	private static final int THREAD_POOL_SIZE = 4;
	private ServerSocket socket;
	private DataCenterInstance[] instances;

	public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) {
		this.socket = socket;
		this.instances = instances;
	}

	// Complete this function
	public void start() throws Exception {
		ExecutorService executorService = Executors
				.newFixedThreadPool(THREAD_POOL_SIZE);
		int index = 0;
		while (true) {
			
			int code = connInfo(instances[index].getUrl());
			// if the connection if not successful, then mark the instance as "die"
			if (code != 200 && !instances[index].isDie()) {
System.out.println("enter new thread");
				instances[index].setDie(true);
				Thread thread = new Thread(new CreatDc(index, instances));
				thread.start();
			}

			// By default, it will send all requests to the first instance
System.out.println(index + " DNS is: " + instances[index].getUrl() + " , status is: " + instances[index].isDie());
			if (!instances[index].isDie()) {
				Runnable requestHandler = new RequestHandler(socket.accept(), instances[index]);
				executorService.execute(requestHandler);
			}

			index++;
			// if index bigger than 2, make it 0
			if (index > 2) {
				index = 0;
			}
		}
	}

	// Get the current rps for each DC VM
	private static int connInfo(String dns) {
		// read log and send bach the current rsp
		try {
			URL u = new URL(dns);
			HttpURLConnection conn;
			conn = (HttpURLConnection) u.openConnection();
			conn.setRequestMethod("GET");
			conn.connect();
			return conn.getResponseCode();
		// any other errors will be considered as unsuccessful connection	
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return 0;
		} catch (ProtocolException e) {
			e.printStackTrace();
			return 0;
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}
}
