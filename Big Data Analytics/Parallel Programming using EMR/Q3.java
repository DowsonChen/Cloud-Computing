import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;

// What was the most popular article on December 18, 2015 from the filtered output? 
// How many daily views did the most popular article get on December 18?
// Run your commands/code to process the output and echo <article_name>\t<daily views>
// to standard output
public class Q3 {
	public static void main(String args[]) {
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));
			// Initialize Variables
			String input;
			String name = null;
			BigInteger maxView = BigInteger.ZERO;
			
			// While we have input on stdin
			while ((input = br.readLine()) != null && input.length() != 0) {
				String[] str = input.split("\t");
				String curName = str[1];
				BigInteger view = new BigInteger(str[19].substring(9));
				if (view.compareTo(maxView) > 0) {
					maxView = view;
					name = curName;
				}
			}
			System.out.println(name + "\t" + maxView.toString());
		} catch (IOException io) {
			io.printStackTrace();
		}
	}
}
