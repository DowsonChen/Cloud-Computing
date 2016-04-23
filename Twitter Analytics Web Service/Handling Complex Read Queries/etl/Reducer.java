import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class Reducer {
    private static final String bannedFile = "banned.txt";
    private static final String stopFile = "common-english-word.txt";
    
    // List of words need to be banned
    private static List<String> bannedWords = new ArrayList<String>();
    
    // List of stop words
    private static List<String> stopWords = new ArrayList<String>();
    
    // Twitter object with reduced fields
    public static class TwitterReduce {
        //private String twi_id = null;
        private long user_id = 0L;
        private String created_time = null;
        private String text = null;

        public TwitterReduce(long user_id, String created_time, String text) {
            this.user_id = user_id;
            this.created_time = created_time;
            this.text = text;
        }
        
        // getters
        /*public String getTwi_id() {
            return twi_id;
        }*/
        public long getUser_id() {
            return user_id;
        }
        public String getCreated_time() {
            return created_time;
        }
        public String getText() {
            return text;
        }
		
        // setters
        /*public void setTwi_id(String twi_id) {
            this.twi_id = twi_id;
        }*/
    }
    
	public static List<String> textSplit(String text) {
    	String textNoURL = text.replaceAll("(https?|ftp):\\/\\/[^\\s/$.?#][^\\s]*", "");
        String[] words = textNoURL.toLowerCase().split("[^a-zA-Z0-9]");
        List<String> wordList = new ArrayList<String>();
        for (String w: words) {
        	if (w.length() > 0 && !bannedWords.contains(w) && !stopWords.contains(w)) {
        		wordList.add(w);
        	}
        }
        return wordList;
	}
	
	public static HashMap<String, Integer> wordCount(List<String> words) {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		for (String w: words) {
			if (!map.containsKey(w)) {
				map.put(w, 1);
			} else {
				map.put(w, map.get(w) + 1);
			}
		}
		return map;
	}
    
    // decipher a word according to rot13
    private static String rot13(String str) {
        String result = "";
        for (int i=0; i < str.length(); ++i) {
            char c = str.charAt(i);
            if (c >= 'a' && c < 'n') {
                c += 13;    
            } else if (c > 'm' && c <= 'z') {
                c -= 13;
            }
            result += c;
        }
        return result;
    }
    
    // Store banned words from file to list of String
    private static void getBannedList() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(bannedFile));      
            String line = br.readLine();
            while (line != null) {
                bannedWords.add(rot13(line));
                line = br.readLine();
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }
   
    // Store Stop words from file to list of String
    private static void getStopList() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(stopFile));      
            String line = br.readLine();
            String[] words = line.split(",");
            for (int i = 0; i < words.length; i++) {
                stopWords.add(words[i]);
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    // escape /n and /t
/*    private static String escape(String str) {
        return str.replace("\n", "\\n").replace("\t", "\\t");
    }*/
    
    public static void main(String[] args) throws JsonSyntaxException, IOException {
        // get words lists
        getBannedList();
        getStopList();
        // read from mapper
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        PrintStream sysout = new PrintStream(System.out, true, "UTF-8");
        Gson gson = new Gson();
        try {
            String line = "";
            String lastId = null;
            while ((line = br.readLine()) != null) {
            	// remove dupilcate twitter ID
                String[] parts = line.split("\t", 2);
                if (lastId == null || !parts[0].equals(lastId)) {
                	lastId = parts[0];
                } else {
                    continue;
                }
                // create twitterReduce object to receive Json, corresponding field will
                // get value automatically.
                TwitterReduce twitterReduce = gson.fromJson(parts[1], TwitterReduce.class);
                
                //twitterReduce.setTwi_id(parts[0]);
                HashMap<String, Integer> wordCount = wordCount(textSplit(twitterReduce.getText()));
                
                // get output Json
                // String json = gson.toJson(twitterReduce);

                // print out result to save in S3 bucket
                if (wordCount.size() != 0) {
	                sysout.print(parts[0] + "\t");
	                sysout.print(twitterReduce.getUser_id() + "\t");
	                sysout.print(twitterReduce.getCreated_time() + "\t");
	                String s = wordCount.toString();
	                sysout.print(s.substring(1, s.length()-1));
	                sysout.print("\n");
                }
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
