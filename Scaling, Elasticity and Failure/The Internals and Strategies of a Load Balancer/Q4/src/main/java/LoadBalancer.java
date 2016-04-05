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
	private static DataCenterInstance[] instances;

	public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) {
		this.socket = socket;
		this.instances = instances;
	}

	// Complete this function
	public void start() throws Exception {
		ExecutorService executorService = Executors
				.newFixedThreadPool(THREAD_POOL_SIZE);
		int index = 0;
		int count = 0;
		while (true) {

			int code = connInfo(instances[index].getUrl());
			if (code != 200 && !instances[index].isDie()) {
				System.out.println("enter new thread");
				instances[index].setDie(true);
				Thread thread = new Thread(new CreatDc(index, instances));
				thread.start();
			}

			// By default, it will send all requests to the first instance
			System.out.println(index + " DNS is: " + instances[index].getUrl()
					+ " , status is: " + instances[index].isDie());
			if (!instances[index].isDie()) {
				Runnable requestHandler = new RequestHandler(socket.accept(),
						instances[index]);
				executorService.execute(requestHandler);
			}

			index++;
			count++;
			if (index > 2) {
				index = 0;
			}
			if (count >= 5) {
				index = currLeastInstace();
				count = 0;
			}
		}
	}

	// Get current the least busy instance
	private static int currLeastInstace() throws InterruptedException, IOException {
		String url0 = instances[0].getUrl();
		String url1 = instances[1].getUrl();
		String url2 = instances[2].getUrl();
		double dc1 = getCpu(url0);
		double dc2 = getCpu(url1);
		double dc3 = getCpu(url2);
		if (dc1 <= dc2) {
			if (dc1 <= dc3) {
				return 0;
			} else {
				return 2;
			}
		} else if (dc2 <= dc3) {
			return 1;
		} else {
			return 2;
		}
	}

	// Get the current CPU for each DC VM
	private static double getCpu(String dns) {
		// read log and send bach the current rsp
		double cpu = 0;
		try {
			URL u = new URL(dns + ":8080/info/cpu");
			HttpURLConnection conn;
			conn = (HttpURLConnection) u.openConnection();
			conn.setRequestMethod("GET");
			conn.connect();

			cpu = 0;
			String[] infoLine = null;
			String line;
			String info = null;
			BufferedReader bf = new BufferedReader(new InputStreamReader(
					conn.getInputStream()));
			while ((line = bf.readLine()) != null) {
				info = line;
			}

			// Split the line with <body> tag
			infoLine = info.split("<body>");
			String[] result = infoLine[1].split("</body>");
			if (result[0].length() != 0) {
				cpu = Double.parseDouble(result[0]);
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
			cpu = 1000;
		} catch (MalformedURLException e) {
			e.printStackTrace();
			cpu = 1000;
		} catch (ProtocolException e) {
			e.printStackTrace();
			cpu = 1000;
		} catch (IOException e) {
			e.printStackTrace();
			cpu = 1000;
		}
		return cpu;
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
