import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;

public class DataCenterInstance {
	private String name;
	private String url;
	private boolean die;

	public DataCenterInstance(String name, String url, boolean die) {
		this.name = name;
		this.url = url;
		this.die = die;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public boolean isDie() {
		return die;
	}

	public void setDie(boolean die) {
		this.die = die;
	}

	/**
	 * Execute the request on the Data Center Instance
	 * @param path
	 * @return URLConnection
	 * @throws IOException
	 */
	public URLConnection executeRequest(String path) throws IOException {
		URLConnection conn = openConnection(path);
		return conn;
	}

	/**
	 * Open a connection with the Data Center Instance
	 * @param path
	 * @return URLConnection
	 * @throws IOException
	 */
	private URLConnection openConnection(String path) throws IOException {
		URL url = new URL(path);
		URLConnection conn = url.openConnection();
		conn.setDoInput(true);
		conn.setDoOutput(false);
        	conn.setConnectTimeout(1000);
        	conn.setReadTimeout(1000);

		return conn;
	}
}
