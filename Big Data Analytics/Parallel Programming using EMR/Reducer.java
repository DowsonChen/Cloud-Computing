import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Reducer {
	
	public static String[] getDate(String str) {
		String[] dates = new String[31];
		String[] datesFormat = new String[31];
		String yearMonth = str.substring(0,6);
		dates[0] = yearMonth + "01";
		int add = 1;
		for (int i = 1; i < 31; i++) {
			int date = Integer.parseInt(dates[0]) + add;
			dates[i] = Integer.toString(date);
			add++;
		}
		for (int i = 0; i < 31; i++) {
			datesFormat[i] = dates[i] + ":";
		}
		return datesFormat;
	}
	
	public static void main(String args[]) {
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));
			// Initialize Variables
			String input;
			String curArticle = null;
			int curView = 0;
			String curDate = null;
			// sum of views per day in 31 days
			int[] sum = new int[31];

			// While we have input on stdin
			while ((input = br.readLine()) != null) {
				try {
					String[] parts = input.split("\t");
					String article = parts[0];
					int view = Integer.parseInt(parts[1]);
					String date = parts[2];
					String yearMonth = date.substring(0,6);
					int firstDate = Integer.parseInt(yearMonth + "01");
					
					int index = Integer.parseInt(date) - firstDate;

					if (curArticle == null) {
						curArticle = article;
						curView = view;
						curDate = date;
						sum[index] = view;
						continue;
					}

					if(curArticle.equals(article)) {
						curView += view;
						sum[index] += view;
						curDate = date;
					} else {
						// if article is diffrent and view more than 100000, print
						if (curView > 100000) {
							String[] dates = getDate(curDate);
							System.out.print(curView + "\t" + curArticle);
							for (int i = 0; i < 31; i++) {
								System.out.print("\t" + dates[i] + sum[i]);
							}
							System.out.println();
						}
						// reset status
						curArticle = article;
						curView = view;
						curDate = date;
						sum = new int[31];
						sum[index] = view;	
					}

				} catch (NumberFormatException e) {
					continue;
				}
			}
			// Print out last article
			if (curArticle != null) {
				if (curView > 100000) {
					String[] dates = getDate(curDate);
					System.out.print(curView + "\t" + curArticle);
					for (int i = 0; i < 31; i++) {
						System.out.print("\t" + dates[i] + sum[i]);
					}
					System.out.println();
				}
			}

	} catch (IOException io) {
			io.printStackTrace();
		}
	}
}