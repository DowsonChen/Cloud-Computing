import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;

public class LoadBalancer {
	private static final int THREAD_POOL_SIZE = 4;
	private final ServerSocket socket;
	private final DataCenterInstance[] instances;

	public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) {
		this.socket = socket;
		this.instances = instances;
	}

	// Complete this function
	public void start() throws IOException, InterruptedException {
		ExecutorService executorService = Executors
				.newFixedThreadPool(THREAD_POOL_SIZE);
		int index = 0;
		int count = 0;
		while (true) {
			// By default, it will send all requests to the first instance
			Runnable requestHandler = new RequestHandler(socket.accept(),
					instances[index]);
			executorService.execute(requestHandler);
			count++;
			index++;
			if (index > 2) {
				index = 0;
			}
			// when round robin five times, check the cpu to make allocation
			if (count >= 5) {
				index = currLeastInstace(instances);
				count = 0;
			}
		}
	}

	// Get current the least busy instance
	private static int currLeastInstace(DataCenterInstance[] instances) throws InterruptedException,
			IOException {
		// get three instances' url
		String dc1Dns = instances[0].getUrl();
		String dc2Dns = instances[0].getUrl();
		String dc3Dns = instances[0].getUrl();
		// get current cpu
		double dc1 = getCpu(dc1Dns);
		double dc2 = getCpu(dc2Dns);
		double dc3 = getCpu(dc3Dns);
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

	// Get the current cpu for each DC VM
	private static double getCpu(String dns) throws InterruptedException,
			IOException {
		// read log and send bach the current rsp
		String dcDns = dns;
		URL u = new URL(dcDns + ":8080/info/cpu");
		HttpURLConnection conn;
		conn = (HttpURLConnection) u.openConnection();
		conn.setRequestMethod("GET");
		conn.connect();

		double cpu = 0;
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
		return cpu;
	}
}
