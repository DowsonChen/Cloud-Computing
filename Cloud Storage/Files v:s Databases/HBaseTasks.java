import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.filter.FilterList;

import java.io.IOException;

public class HBaseTasks {

    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "172.31.2.116";
    /**
     * The name of your HBase table.
     */
    private static String tableName = "songdata";
    /**
     * HTable handler.
     */
    private static HTableInterface songsTable;
    /**
     * HBase connection.
     */
    private static HConnection conn;
    /**
     * Byte representation of column family.
     */
    private static byte[] bColFamily = Bytes.toBytes("data");
    /**
     * Logger.
     */
    private final static Logger logger = Logger.getRootLogger();


    /**
     * Initialize HBase connection.
     * @throws IOException
     */
    private static void initializeConnection() throws IOException{
        // Remember to set correct log level to avoid unnecessary output.
        logger.setLevel(Level.ERROR);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":60000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
	    if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
		    System.out.print("HBase not configured!");
		    return;
	    }
		conn = HConnectionManager.createConnection(conf);
	    // while (true) {
	        // try {
				songsTable = conn.getTable(Bytes.toBytes(tableName));
				// if (songsTable != null) {
					// break;
				// }
			// } catch (Exception e) {
				// continue;
			// }	
	    // }
    }

    /**
     * Clean up resources.
     * @throws IOException
     */
    private static void cleanup() throws IOException {
        if (songsTable != null) {
            songsTable.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * You should complete the missing parts in the following method. Feel free to add helper functions if necessary.
     *
     * For all questions, output your answer in ONE single line, i.e. use System.out.print().
     *
     * @param args The arguments for main method.
     */
    public static void main(String[] args) throws IOException {
        initializeConnection();
        switch (args[0]) {
            case "demo":
                demo();
                break;
            case "q17":
                q17();
                break;
            case "q18":
                q18();
                break;
            case "q19":
                q19();
                break;
            case "q20":
                q20();
                break;
            case "q21":
                q21();
        }
        cleanup();
    }

    /**
     * This is a demo of how to use HBase Java API. It will print all the artist_names starting with "The Beatles".
     * @throws IOException
     */
    private static void demo() throws IOException {
        Scan scan = new Scan();
        byte[] bCol = Bytes.toBytes("artist_name");
        scan.addColumn(bColFamily, bCol);
        RegexStringComparator comp = new RegexStringComparator("^The Beatles.*");
        Filter filter = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp);
        scan.setFilter(filter);
        scan.setBatch(10);
        ResultScanner rs = songsTable.getScanner(scan);
        int count = 0;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            count ++;
            System.out.println(Bytes.toString(r.getValue(bColFamily, bCol)));
        }
        System.out.println("Scan finished. " + count + " match(es) found.");
        rs.close();
    }

    /**
     * Question 17.
     *
     * What was that song whose name started with "Total" and ended with "Water"?
     * Write an HBase query that finds the track that the person is looking for.
     * The title starts with "Total" and ends with "Water", both are case sensitive.
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q17() throws IOException{
        Scan scan = new Scan();
        byte[] bCol = Bytes.toBytes("title");
        scan.addColumn(bColFamily, bCol);
        RegexStringComparator start = new RegexStringComparator("^Total");
        RegexStringComparator end = new RegexStringComparator("Water$");
        Filter filterStart = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, start);
        Filter filterend = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, end);
        // Use filter list to do multi filters
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(filterStart);
        list.addFilter(filterend);
        scan.setFilter(list);
        ResultScanner rs = songsTable.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            System.out.print(Bytes.toString(r.getValue(bColFamily, bCol)));
        }
        rs.close();
    }

    /**
     * Question 18.
     *
     * I don't remember the exact title, it was that song by "Kanye West", and the
     * title started with either "Apologies" or "Confessions". Not sure which...
     * Write an HBase query that finds the track that the person is looking for.
     * The artist_name contains "Kanye West", and the title starts with either
     * "Apologies" or "Confessions" (Case sensitive).
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q18() throws IOException{
        Scan scan = new Scan();
        byte[] titleCol = Bytes.toBytes("title");
        byte[] artistCol = Bytes.toBytes("artist_name");
        scan.addFamily(bColFamily);
        RegexStringComparator start = new RegexStringComparator("^(Apologies.*|Confessions.*)");
        // RegexStringComparator start = new RegexStringComparator("^Apologies.*" | "^Confessions.*");
        RegexStringComparator artist = new RegexStringComparator(".*Kanye West.*");
        Filter filter1 = new SingleColumnValueFilter(bColFamily, titleCol, CompareFilter.CompareOp.EQUAL, start);
        Filter filter2 = new SingleColumnValueFilter(bColFamily, artistCol, CompareFilter.CompareOp.EQUAL, artist);
        // Use filter list to do multi filters
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(filter1);
        list.addFilter(filter2);
        scan.setFilter(list);
        ResultScanner rs = songsTable.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            System.out.print(Bytes.toString(r.getValue(bColFamily, titleCol)));
        }
        rs.close();
    }

    /**
     * Question 19.
     *
     * There was that new track by "Bob Marley" that was really long. Do you know?
     * Write an HBase query that finds the track the person is looking for.
     * The artist_name has a prefix of "Bob Marley", duration no less than 400,
     * and year 2000 and onwards (Case sensitive).
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q19() throws IOException{
        Scan scan = new Scan();
        byte[] artistCol = Bytes.toBytes("artist_name");
        byte[] duritionCol = Bytes.toBytes("duration");
        byte[] yearCol = Bytes.toBytes("year");
        byte[] titleCol = Bytes.toBytes("title");
        scan.addFamily(bColFamily);
        RegexStringComparator start = new RegexStringComparator("^Bob Marley.*");
        Filter filter1 = new SingleColumnValueFilter(bColFamily, artistCol, CompareFilter.CompareOp.EQUAL, start);
        Filter filter2 = new SingleColumnValueFilter(bColFamily, duritionCol, CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("400"));
        Filter filter3 = new SingleColumnValueFilter(bColFamily, yearCol, CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("2000"));
        // Use filter list to do multi filters
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(filter1);
        list.addFilter(filter2);
        list.addFilter(filter3);
        scan.setFilter(list);
        ResultScanner rs = songsTable.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            System.out.print(Bytes.toString(r.getValue(bColFamily, titleCol)));
        }
        rs.close();        
    }

    /**
     * Question 20.
     *
     * I heard a really great song about "Family" by this really cute singer,
     * I think his name was "Consequence" or something...
     * Write an HBase query that finds the track the person is looking for.
     * The track has an artist_hotttnesss of at least 1, and the artist_name
     * contains "Consequence". Also, the title contains "Family" (Case sensitive).
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q20() throws IOException{
        Scan scan = new Scan();
        byte[] hotCol = Bytes.toBytes("artist_hotttnesss");
        byte[] artistCol = Bytes.toBytes("artist_name");
        byte[] titleCol = Bytes.toBytes("title");
        scan.addFamily(bColFamily);
        RegexStringComparator name = new RegexStringComparator(".*Consequence.*");
        RegexStringComparator title = new RegexStringComparator(".*Family.*");
        Filter filter1 = new SingleColumnValueFilter(bColFamily, artistCol, CompareFilter.CompareOp.EQUAL, name);
        Filter filter2 = new SingleColumnValueFilter(bColFamily, hotCol, CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("1"));
        Filter filter3 = new SingleColumnValueFilter(bColFamily, titleCol, CompareFilter.CompareOp.EQUAL, title);
        // Use filter list to do multi filters
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(filter1);
        list.addFilter(filter2);
        list.addFilter(filter3);
        scan.setFilter(list);
        ResultScanner rs = songsTable.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            System.out.print(Bytes.toString(r.getValue(bColFamily, titleCol)));
        }
        rs.close();   
    }

    /**
     * Question 21.
     *
     * Hey what was that "Love" song that "Gwen Guthrie" came out with in 1990?
     * No, no, it wasn't the sad one, nothing "Bitter" or "Never"...
     * Write an HBase query that finds the track the person is looking for.
     * The track has an artist_name prefix of "Gwen Guthrie", the title contains "Love"
     * but does NOT contain "Bitter" or "Never", the year equals to 1990.
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q21() throws IOException{
        Scan scan = new Scan();
        byte[] artistCol = Bytes.toBytes("artist_name");
        byte[] titleCol = Bytes.toBytes("title");
        byte[] yearCol = Bytes.toBytes("year");
        scan.addFamily(bColFamily);
        RegexStringComparator name = new RegexStringComparator("^Gwen Guthrie.*");
        RegexStringComparator title = new RegexStringComparator("^(?!.*(Bitter|Never))(?=.*Love).*");
        Filter filter1 = new SingleColumnValueFilter(bColFamily, artistCol, CompareFilter.CompareOp.EQUAL, name);
        Filter filter2 = new SingleColumnValueFilter(bColFamily, titleCol, CompareFilter.CompareOp.EQUAL, title);
        Filter filter3 = new SingleColumnValueFilter(bColFamily, yearCol, CompareFilter.CompareOp.EQUAL, Bytes.toBytes("1990"));
        // Use filter list to do multi filters
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(filter1);
        list.addFilter(filter2);
        list.addFilter(filter3);
        scan.setFilter(list);
        ResultScanner rs = songsTable.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            System.out.print(Bytes.toString(r.getValue(bColFamily, titleCol)));
        }
        rs.close(); 
    }

}
