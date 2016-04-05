import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;

public class Q4 {
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
				BigInteger view = new BigInteger(str[0]);
				BigInteger bi = new BigInteger(str[2].substring(9));
				if (bi.signum() == 0) {
					if (view.compareTo(maxView) > 0) {
						maxView = view;
						name = str[1];
					}
				}
			}

			System.out.println(name);
		} catch (IOException io) {
			io.printStackTrace();
		}
	}
}
