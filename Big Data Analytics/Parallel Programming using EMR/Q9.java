import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Q9 {
	public static void main(String args[]) {
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));
			// Initialize Variables
			String input = br.readLine();
			int globalMax = 0;
			int maxNum = 0;

			// While we have input on stdin
			while ((input = br.readLine()) != null) {
				if (input.length() > 0) {
				try {
					int curMax = 0;
					int curView = 0;
					int max = 0;
					
					String[] str = input.split("\t");
					for (int i = 2; i < 33; i++) {
						if (Integer.parseInt(str[i].substring(9)) < curView) {
							curMax++;
						} else if (Integer.parseInt(str[i].substring(9)) > curView) {
							curMax = 0;
						}
						if (curMax > max) {
							max = curMax;
						}
						curView = Integer.parseInt(str[i].substring(9));
					}

				if (max == globalMax) {
					maxNum++;
				}
				if (max > globalMax) {
					globalMax = max;
					maxNum = 1;
				}
				} catch (NumberFormatException e) {
					continue;
				}
				}	
			}
			System.out.println(maxNum);
	} catch (IOException io) {
			io.printStackTrace();
		}
	}
}
