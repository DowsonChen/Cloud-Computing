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
		while (true) {
			// By default, it will send all requests to the first instance
			Runnable requestHandler = new RequestHandler(socket.accept(),
					instances[index]);
			executorService.execute(requestHandler);
			index++;
			// when index bigger than 2 make it 0
			if (index > 2) {
				index = 0;
			}
		}
	}
}
