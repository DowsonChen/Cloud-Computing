import com.microsoft.azure.management.compute.ComputeManagementClient;
import com.microsoft.azure.management.compute.ComputeManagementService;
import com.microsoft.azure.management.compute.models.*;
import com.microsoft.azure.management.network.NetworkResourceProviderClient;
import com.microsoft.azure.management.network.NetworkResourceProviderService;
import com.microsoft.azure.management.network.models.AzureAsyncOperationResponse;
import com.microsoft.azure.management.network.models.PublicIpAddressGetResponse;
import com.microsoft.azure.management.resources.ResourceManagementClient;
import com.microsoft.azure.management.resources.ResourceManagementService;
import com.microsoft.azure.management.storage.StorageManagementClient;
import com.microsoft.azure.management.network.models.DhcpOptions;
import com.microsoft.azure.management.storage.StorageManagementService;
import com.microsoft.azure.management.network.models.VirtualNetwork;
import com.microsoft.azure.utility.*;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Math;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

public class AzureVMApiDemo {
    private static ResourceManagementClient resourceManagementClient;
    private static StorageManagementClient storageManagementClient;
    private static ComputeManagementClient computeManagementClient;
    private static NetworkResourceProviderClient networkResourceProviderClient;

    // the source URI of VHD
    private static String sourceVhdUri = "";

    // configuration for your application token
    private static String baseURI = "https://management.azure.com/";
    private static String basicURI = "https://management.core.windows.net/";
    private static String endpointURL = "https://login.windows.net/";

    private static String subscriptionId = "";
    private static String tenantID = "";
    private static String applicationID = "";
    private static String applicationKey = "";

    // configuration for your resource account/storage account
    private static String storageAccountName = "";
    private static String resourceGroupNameWithVhd = "";
    private static String size = "";
    private static String region = "EastUs";
    private static String vmName = "";
    private static String resourceGroupName = "";

    // configuration for your virtual machine
    private static String adminName = "ubuntu";
    /**
      * Password requirements:
      * 1) Contains an uppercase character
      * 2) Contains a lowercase character
      * 3) Contains a numeric digit
      * 4) Contains a special character.
      */
    private static String adminPassword = "Cloud@123";

    // *********************************************
    // my parameters
	private static final String[] lgstr = {"Project2.1", "myproject2", 
		"cc15619p21lgv10-osDisk.f6be8828-8cab-45ae-a611-904aeeef3c9e.vhd", "fd9e2c65-dd84-4aa6-93d2-865e011e1eee",
		"38dc8aaa-e50c-486d-a8a9-3d21112d05e7", "e326ebe2-63f2-48dc-9242-1df093c50588", "123", "Standard_D1"};
	
	private static final String[] dcstr = {"Project2.1", "myproject2", 
		"cc15619p21dcv5-osDisk.e27faca3-f177-40ea-a740-9a1838326ae6.vhd", "fd9e2c65-dd84-4aa6-93d2-865e011e1eee",
		"38dc8aaa-e50c-486d-a8a9-3d21112d05e7", "e326ebe2-63f2-48dc-9242-1df093c50588", "123", "Standard_A1"};
	
	private static String andrewId = "";
	private static String subPassword = "";
	
	// set some re-use url part to static string
	static final String http = new String("http://");
	static final String pwd = new String("/password?passwd=");
	static final String andId = new String("&andrewid=");
	static final String submitDC = new String("/test/horizontal?dns=");
	static String testID = new String("");
	static final String addVM = new String("/test/horizontal/add?dns=");
	
	private static String targetUrl;
	private static String lgDNS;
    // **********************************************
    
    public AzureVMApiDemo() throws Exception{
        Configuration config = createConfiguration();
        resourceManagementClient = ResourceManagementService.create(config);
        storageManagementClient = StorageManagementService.create(config);
        computeManagementClient = ComputeManagementService.create(config);
        networkResourceProviderClient = NetworkResourceProviderService.create(config);
    }

    public static Configuration createConfiguration() throws Exception {
        // get token for authentication
        String token = AuthHelper.getAccessTokenFromServicePrincipalCredentials(
                        basicURI,
                        endpointURL,
                        tenantID,
                        applicationID,
                        applicationKey).getAccessToken();

        // generate Azure sdk configuration manager
        return ManagementConfiguration.configure(
                null, // profile
                new URI(baseURI), // baseURI
                subscriptionId, // subscriptionId
                token// token
                );
    }

    /***
     * Create a virtual machine given configurations.
     *
     * @param resourceGroupName: a new name for your virtual machine [customized], will create a new one if not already exist
     * @param vmName: a PUBLIC UNIQUE name for virtual machine
     * @param resourceGroupNameWithVhd: the resource group where the storage account for VHD is copied
     * @param sourceVhdUri: the Uri for VHD you copied
     * @param instanceSize
     * @param subscriptionId: your Azure account subscription Id
     * @param storageAccountName: the storage account where you VHD exist
     * @return created virtual machine IP
     */
    public static ResourceContext createVM (
        String resourceGroupName,
        String vmName,
        String resourceGroupNameWithVhd,
        String sourceVhdUri,
        String instanceSize,
        String subscriptionId,
        String storageAccountName) throws Exception {

        ResourceContext contextVhd = new ResourceContext(
                region, resourceGroupNameWithVhd, subscriptionId, false);
        ResourceContext context = new ResourceContext(
                region, resourceGroupName, subscriptionId, false);

        ComputeHelper.createOrUpdateResourceGroup(resourceManagementClient,context);
        context.setStorageAccountName(storageAccountName);
        contextVhd.setStorageAccountName(storageAccountName);
        context.setStorageAccount(StorageHelper.getStorageAccount(storageManagementClient,contextVhd));

        if (context.getNetworkInterface() == null) {
            if (context.getPublicIpAddress() == null) {
                NetworkHelper
                    .createPublicIpAddress(networkResourceProviderClient, context);
            }
            if (context.getVirtualNetwork() == null) {
                NetworkHelper
                    .createVirtualNetwork(networkResourceProviderClient, context);
            }

            VirtualNetwork vnet =  context.getVirtualNetwork();

            // set DhcpOptions
            DhcpOptions dop = new DhcpOptions();
            ArrayList<String> dnsServers = new ArrayList<String>(2);
            dnsServers.add("8.8.8.8");
            dop.setDnsServers(dnsServers);
            vnet.setDhcpOptions(dop);

            try {
                AzureAsyncOperationResponse response = networkResourceProviderClient.getVirtualNetworksOperations()
                    .createOrUpdate(context.getResourceGroupName(), context.getVirtualNetworkName(), vnet);
            } catch (ExecutionException ee) {
                if (ee.getMessage().contains("RetryableError")) {
                    AzureAsyncOperationResponse response2 = networkResourceProviderClient.getVirtualNetworksOperations()
                        .createOrUpdate(context.getResourceGroupName(), context.getVirtualNetworkName(), vnet);
                } else {
                    throw ee;
                }
            }


            NetworkHelper
                .createNIC(networkResourceProviderClient, context, context.getVirtualNetwork().getSubnets().get(0));

            NetworkHelper
                .updatePublicIpAddressDomainName(networkResourceProviderClient, resourceGroupName, context.getPublicIpName(), vmName);
        }

        System.out.println("[15319/15619] "+context.getPublicIpName());
        System.out.println("[15319/15619] Start Create VM...");

        try {
            // name for your VirtualHardDisk
            String osVhdUri = ComputeHelper.getVhdContainerUrl(context) + String.format("/os%s.vhd", vmName);

            VirtualMachine vm = new VirtualMachine(context.getLocation());

            vm.setName(vmName);
            vm.setType("Microsoft.Compute/virtualMachines");
            vm.setHardwareProfile(createHardwareProfile(context, instanceSize));
            vm.setStorageProfile(createStorageProfile(osVhdUri, sourceVhdUri));
            vm.setNetworkProfile(createNetworkProfile(context));
            vm.setOSProfile(createOSProfile(adminName, adminPassword, vmName));

            context.setVMInput(vm);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        // Remove the resource group will remove all assets (VM/VirtualNetwork/Storage Account etc.)
        // Comment the following line to keep the VM.
        // resourceManagementClient.getResourceGroupsOperations().beginDeleting(context.getResourceGroupName());
        // computeManagementClient.getVirtualMachinesOperations().beginDeleting(resourceGroupName,"project2.2");
        return context;
        }

    /***
     * Check public IP address of virtual machine
     *
     * @param context
     * @param vmName
     * @return public IP
     */
    public static String checkVM(ResourceContext context, String vmName) {
        String ipAddress = null;

        try {
            VirtualMachine vmHelper = ComputeHelper.createVM(
                    resourceManagementClient, computeManagementClient, networkResourceProviderClient, storageManagementClient,
                    context, vmName, "ubuntu", "Cloud@123").getVirtualMachine();

            System.out.println("[15319/15619] "+vmHelper.getName() + " Is Created :)");
            while(ipAddress == null) {
                PublicIpAddressGetResponse result = networkResourceProviderClient.getPublicIpAddressesOperations().get(resourceGroupName, context.getPublicIpName());
                ipAddress = result.getPublicIpAddress().getIpAddress();
                Thread.sleep(10);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return ipAddress;
    }

    /***
     * Create a HardwareProfile for virtual machine
     *
     * @param context
     * @param instanceSize
     * @return created HardwareProfile
     */
    public static HardwareProfile createHardwareProfile(ResourceContext context, String instanceSize) {
        HardwareProfile hardwareProfile = new HardwareProfile();
        if (context.getVirtualMachineSizeType()!=null && !context.getVirtualMachineSizeType().isEmpty()) {
            hardwareProfile.setVirtualMachineSize(context.getVirtualMachineSizeType());
        } else {
            hardwareProfile.setVirtualMachineSize(instanceSize);
        }
        return hardwareProfile;
    }

    /***
     * Create a StorageProfile for virtual machine
     *
     * @param osVhdUri
     * @param sourceVhdUri
     * @return created StorageProfile
     */
    public static StorageProfile createStorageProfile(String osVhdUri, String sourceVhdUri) {
        StorageProfile storageProfile = new StorageProfile();

        VirtualHardDisk vHardDisk = new VirtualHardDisk();
        vHardDisk.setUri(osVhdUri);
        //set source image
        VirtualHardDisk sourceDisk = new VirtualHardDisk();
        sourceDisk.setUri(sourceVhdUri);

        OSDisk osDisk = new OSDisk("osdisk", vHardDisk, DiskCreateOptionTypes.FROMIMAGE);
        osDisk.setSourceImage(sourceDisk);
        osDisk.setOperatingSystemType(OperatingSystemTypes.LINUX);
        osDisk.setCaching(CachingTypes.NONE);

        storageProfile.setOSDisk(osDisk);

        return storageProfile;
    }

    /***
     * Create a NetworkProfile for virtual machine
     *
     * @param context
     * @return created NetworkProfile
     */
    public static NetworkProfile createNetworkProfile(ResourceContext context) {
        NetworkProfile networkProfile = new NetworkProfile();
        NetworkInterfaceReference nir = new NetworkInterfaceReference();
        nir.setReferenceUri(context.getNetworkInterface().getId());
        ArrayList<NetworkInterfaceReference> nirs = new ArrayList<NetworkInterfaceReference>(1);
        nirs.add(nir);
        networkProfile.setNetworkInterfaces(nirs);

        return networkProfile;
    }

    /***
     * Create a OSProfile for virtual machine
     *
     * @param adminName
     * @param adminPassword
     * @param vmName
     * @return created OSProfile
     */
    public static OSProfile createOSProfile(String adminName, String adminPassword, String vmName) {
        OSProfile osProfile = new OSProfile();
        osProfile.setAdminPassword(adminPassword);
        osProfile.setAdminUsername(adminName);
        osProfile.setComputerName(vmName);

        return osProfile;
    }

    /**
     * args0: resource group
     * args1: storage account
     * args2: image name
     * args3: subscription ID
     * args4: tenant ID
     * args5: application ID
     * args6: application Key
     * args7: vm size
     */
    public static String getDNS(String[] str) throws Exception {
        String seed = String.format("%d%d", (int) System.currentTimeMillis()%1000, (int)(Math.random()*1000));
        vmName = String.format("cloud%s%s", seed, "vm");
        resourceGroupName = String.format("cloud%s%s", seed, "ResourceGroup");

        resourceGroupNameWithVhd = str[0].trim();
        storageAccountName = str[1].trim();
        sourceVhdUri = String.format("https://%s.blob.core.windows.net/system/Microsoft.Compute/Images/vhds/%s", storageAccountName, str[2].trim());
        subscriptionId = str[3].trim();
        tenantID = str[4].trim();
        applicationID = str[5].trim();
        applicationKey = str[6].trim();
        size = str[7].trim();

        AzureVMApiDemo vm = new AzureVMApiDemo();
        
        System.out.println("[15319/15619] Configured");

        ResourceContext context = createVM (
            resourceGroupName,
            vmName,
            resourceGroupNameWithVhd,
            sourceVhdUri,
            size,
            subscriptionId,
            storageAccountName);

        System.out.println(checkVM(context, vmName));
        
        return vmName + ".eastus.cloudapp.azure.com";
    }
    
    // command line andrewID and submission password as parameters
    public static void main(String[] args) throws Exception {
    	
    	andrewId = args[0].trim();
    	subPassword = args[1].trim();
    	
    	// create vm and get vm DNS
    	lgDNS = getDNS(lgstr);
    	
    	// create dc and get vm DNS
    	String dc1DNS = getDNS(dcstr);
    	
    	// authenticate with the load generator
        targetUrl = http + lgDNS + pwd + subPassword + andId + andrewId;
        System.out.println(targetUrl);
        submitRequest(targetUrl);

        Thread.sleep(5000);
        System.out.println("5s sleep");
        
//      submit the data center VM's DNS name to the load generator
        targetUrl =  http + lgDNS + submitDC + dc1DNS;
        System.out.println(targetUrl);
        submitRequest(targetUrl);
//        Solution.submitRequest(targetUrl);
        
        Thread.sleep(5000);
        System.out.println("5s sleep");
        
        // get test ID
        while (testID.length() == 0) {
            testID = getTestID();
        }
        System.out.println("test ID is:" + testID);
        
//         check if need to add dc vm
        
        double rps = 0.00;
        System.out.println("first rps is:" + rps);
        while (rps < 3000) {
        	Thread.sleep(100000);
        	// create a new dc vm and add to lg
        	String dcDNS = getDNS(dcstr);
        	targetUrl = http + lgDNS + addVM + dcDNS;
        	submitRequest(targetUrl);
        	System.out.println("add a new dc");
//        	// get current rps
        	rps = getLastLog();
        	System.out.println("current rps is:" + rps);
        }
        
        return;
    }
    
    // submit url request
	private static void submitRequest(String url) throws MalformedURLException, InterruptedException {
		URL u = new URL(url);
		HttpURLConnection con;
        
        while (true) {
        	try {
        		con = (HttpURLConnection) u.openConnection();
                con.setRequestMethod("GET");
                con.connect();
        		if (con.getResponseCode() != 200) {
        			Thread.sleep(10000);
        			System.out.println("current status code is:" + con.getResponseCode());
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
   
	// read log and send bach the current rsp
    private static double getLastLog() throws IOException, InterruptedException {
    	String viewLog = new String("/log?name=test." + testID + ".log");
		URL u = new URL(http + lgDNS + viewLog);
		System.out.println("view log url: " + http + lgDNS + viewLog);
        HttpURLConnection conn;
        
        while (true) {
        	try {
        		conn = (HttpURLConnection) u.openConnection();
                conn.setRequestMethod("GET");
                conn.connect();
        		if (conn.getResponseCode() != 200) {
        			Thread.sleep(10000);
        			System.out.println("current status code is:" + conn.getResponseCode());
        			conn = (HttpURLConnection) u.openConnection();
        			conn.setRequestMethod("GET");
        			conn.connect();
        			continue;
        		} else {
        	        System.out.println("success-------------------------" + conn.getResponseCode());
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

		double rps = 0;
		ArrayList<Double> lastLog = new ArrayList<Double>();
		
        BufferedReader bf = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		String line;
		
//		StringBuilder builder = new StringBuilder();
//		while ((line = bf.readLine()) != null && !line.startsWith(";")) {
//			builder.append(line);
//		}
//		String[] recordList = builder.toString().split("\n\n");
//		String lastRecord = recordList[recordList.length - 1];
//		String[] lastRecordList = lastRecord.split("\n");
//		for (String item: lastRecordList) {
//			if (item.contains("=")) {
//				rps += Double.parseDouble(item.split("=")[1]);
//			}
//		}
        while ((line = bf.readLine()) != null) {
           if (line.startsWith("[Minute")) {
        	   lastLog.clear();
           }
           if (line.startsWith("cloud-")) {
               double request = Double.parseDouble(line.substring(line.indexOf("=")+1, line.length()));
               lastLog.add(request);
           }
        }
        if (lastLog.size() != 0) {
            for (int i = 0; i < lastLog.size(); ++i) {
                rps += lastLog.get(i);
            }
        }
        
        bf.close();
		return rps;
    }
    
    private static String getTestID() throws IOException, InterruptedException {
        URL u = new URL(http + lgDNS + "/log");
        HttpURLConnection conn;
        
        while (true) {
        	try {
        		conn = (HttpURLConnection) u.openConnection();
                conn.setRequestMethod("GET");
                conn.connect();
        		if (conn.getResponseCode() != 200) {
        			Thread.sleep(10000);
        			System.out.println("current status code is:" + conn.getResponseCode());
        			conn = (HttpURLConnection) u.openConnection();
        			conn.setRequestMethod("GET");
        			conn.connect();
        			continue;
        		} else {
        	        System.out.println("success-------------------------" + conn.getResponseCode());
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
        
		ArrayList<String> lastLog = new ArrayList<String> ();
		String id;
		String line;
        
        BufferedReader bf = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		
		// get current line
		while ((line = bf.readLine()) != null) {
			lastLog.add(line);
		}
		
		String str = lastLog.get(lastLog.size() - 1);
		String[] part = str.split("\\.");
		id = part[1];
		return id;
    }
}

