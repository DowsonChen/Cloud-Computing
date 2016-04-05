import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;


public class CreatDc implements Runnable{

	private static final String[] dcstr = {"Project2.2", "dowsonstorage", 
		"cc15619p22dcv6-osDisk.b0c453f3-f75f-4a2d-bd9c-ae055b830124.vhd", "fd9e2c65-dd84-4aa6-93d2-865e011e1eee",
		"38dc8aaa-e50c-486d-a8a9-3d21112d05e7", "e326ebe2-63f2-48dc-9242-1df093c50588", "123", "Standard_A1"};
	
	String dcDNS;
	int index;
	private DataCenterInstance[] instances;
	
	public CreatDc(int index, DataCenterInstance[] instances) {
		this.index = index;
		this.instances = instances;
	}
	
	public void run() {
		// create dc and get vm DNS
		try {
			dcDNS = AzureVMApiDemo.getDNS(dcstr);
			connInfo(dcDNS);
			instances[index].setDie(false);
			instances[index].setUrl(dcDNS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	private static void connInfo(String dns) throws IOException {
		// read log and send bach the current rsp
		String dcDns = dns;
		URL u = new URL(dcDns);
		HttpURLConnection con;
	    while (true) {
	    	try {
	    		con = (HttpURLConnection) u.openConnection();
	            con.setRequestMethod("GET");
	            con.connect();
	    		if (con.getResponseCode() != 200) {
	    			con = (HttpURLConnection) u.openConnection();
	    			con.setRequestMethod("GET");
	    			con.connect();
	    			continue;
	    		} else {
	    	        System.out.println("success-------------------------" + con.getResponseCode());
	    			break;
	    		}
	    	}catch (ConnectException e){
	    		continue;
	    	} catch (ProtocolException e) {
	    		continue;
			} catch (IOException e) {
				continue;
			}
	    }
	}
}