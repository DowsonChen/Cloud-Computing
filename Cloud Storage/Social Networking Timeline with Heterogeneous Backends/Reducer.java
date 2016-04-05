import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class Reducer {
	
	public static void main(String args[]) {
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			// Initialize Variables
			String input;
//			int prevFollowee = -1;
			int prevFollower = -1;
			ArrayList<Integer> list = new ArrayList<Integer>();

			// While we have input on stdin
			while ((input = br.readLine()) != null) {
				try {
					String[] parts = input.split("\\s+");
					int followee = Integer.parseInt(parts[0]);
					int follower = Integer.parseInt(parts[1]);
					if (prevFollower == -1) {
					    prevFollower = follower;
						list.add(followee);
					} else {
	                    if(prevFollower == follower) {
	                        list.add(followee);
	                    } else {
	                        StringBuilder sb = new StringBuilder();
	                        sb.append(prevFollower + "\t");
	                        for (int i = 0; i < list.size() - 1; i++) {
	                            sb.append(list.get(i) + ",");
	                        }
	                        sb.append(list.get(list.size() - 1));
	                        System.out.println(sb);
	                        list.clear();
	                        prevFollower = follower;
	                        list.add(followee);
	                    }					    
					}

				} catch (NumberFormatException e) {
					continue;
				}
			}
			
            if (prevFollower != -1) {
                StringBuilder sb = new StringBuilder();
                sb.append(prevFollower + "\t");
                for (int i = 0; i < list.size() - 1; i++) {
                    sb.append(list.get(i) + ",");
                }
                sb.append(list.get(list.size() - 1));
                System.out.println(sb);
            }

	} catch (IOException io) {
			io.printStackTrace();
		}
	}
}