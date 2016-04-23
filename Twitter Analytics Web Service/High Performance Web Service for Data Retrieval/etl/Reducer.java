import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class Reducer {
    private static final String bannedFile = "banned.txt";
    private static final String stopFile = "common-english-word.txt";
    private static final String sentimentFile = "afinn.txt";
    
    // List of words need to be banned
    private static List<String> bannedWords = new ArrayList<String>();
    
    // List of stop words
    private static List<String> stopWords = new ArrayList<String>();
    
    // save sentiment word and corresponding score
    private static HashMap<String, String> map = new HashMap<String, String>();
    
    public static class TwitterReduce {
        private String twi_id = null;
        private long user_id = 0L;
        private String created_time = null;
        private String text = null;
        private List<String> tags;
        private double sentiment = 0;
        //private String censoredText = null;

        public TwitterReduce(long user_id, String created_time, String text, List<String> tags) {
            this.user_id = user_id;
            this.created_time = created_time;
            this.text = text;
            this.tags = tags;
        }
        // get methods
        public String getTwi_id() {
            return twi_id;
        }
        public long getUser_id() {
            return user_id;
        }
        public String getCreated_time() {
            return created_time;
        }
        public String getText() {
            return text;
        }
        public List<String> getTags() {
            return tags;
        }
        public double getSentiment() {
            return sentiment;
        }
        // set methods
        public void setTwi_id(String twi_id) {
            this.twi_id = twi_id;
        }
        public void setText(String text) {
            this.text = text;
        }
        public void setSentiment(double sentiment) {
            this.sentiment = sentiment;
        }    
    }
    
    public static String[] textSplit(String text) {
        String[] words = text.toLowerCase().split("[^a-zA-Z0-9]");
        return words;
    }
    
    // calculate the score of current twitter
    private static int getScore(String[] strs) {
        int score = 0;
        for(int i = 0; i < strs.length; i++) {
            if (map.containsKey(strs[i])) {
                score += Integer.parseInt(map.get(strs[i]));
            }
        }
        return score;
    }
    
    // calculate the Effective Word Count of current twitter
    private static int getCount(String[] strs) {
        int count = 0;
        for(int i = 0; i < strs.length; i++) {
            if (strs[i].length() > 0 && !stopWords.contains(strs[i].toLowerCase())) {
                count ++;
            }
        }
        return count;
    }
    
    // calculate the sentiment density, three decimals

    private static double getDensity(int count, int score) {
        if (count == 0) {
            return Double.parseDouble(String.format("%.3f", 0.000));
        }
        double density = (double)score / (double)count;
        double result = Double.parseDouble(String.format("%.3f", density));
        return result;
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
    // Cersor text with Regex pattern string
    private static String wordsCensor(String text, long user_id) {
        String patternString = getPatternString(bannedWords);
        String[] words = text.split("[^a-zA-Z0-9]");
        String[] lowerCaseWords = text.toLowerCase().split("[^a-zA-Z0-9]");
        
        // Check words from lowercase word array
        for (int i = 0; i < words.length; ++i) {
            try {
                if (Pattern.matches(patternString, lowerCaseWords[i])) {
                    int pos = text.indexOf(words[i]);
                    while (isInOtherWord(i, pos, text, words)) {
                        pos = text.indexOf(words[i], pos + 1);
                    }
                    // Change text to new text with banned word censord
                    text = text.substring(0, pos)
                            + asterisk(words[i])
                            + text.substring(pos + words[i].length());
                }
            } catch (Exception e) {
                System.err.println("Error user_id: " + user_id);
                System.err.println("Error text: " + text);
            }
        }
        return text;
    }
    
    private static boolean isInOtherWord(int i, int pos, String text, String[] words) {
        if (pos == 0) {
            String c = String.valueOf(text.charAt(pos + words[i].length()));
            return c.matches("[a-zA-Z0-9]");
        } else if (pos + words[i].length() == text.length()) {
            String c = String.valueOf(text.charAt(pos - 1));
            return c.matches("[a-zA-Z0-9]");
        } else {
            String c1 = String.valueOf(text.charAt(pos + words[i].length()));
            String c2 = String.valueOf(text.charAt(pos - 1));
            return c1.matches("[a-zA-Z0-9]") || c2.matches("[a-zA-Z0-9]");
        }
    }

    // Chage the middle of censored word to asterisk
    private static String asterisk(String word) {
        int n = word.length();
        char firstC = word.charAt(0);
        char lastC = word.charAt(n-1);
        String stars = new String(new char[n-2]).replace("\0", "*");
        String newWord = String.valueOf(firstC) + stars + String.valueOf(lastC);
        return newWord;
    }
    // save all banned words into a Regex pattern string
    private static String getPatternString (List<String> words) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(words.get(0));
        for (int i = 1; i < words.size(); ++i) {
            sb.append("|");
            sb.append(words.get(i));            
        }
        sb.append(")"); 
        return sb.toString();
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
    
 // read the sentiment file and save score
    private static void getSentimentMap() {
        try {
            FileReader fr = new FileReader(sentimentFile);
            BufferedReader br = new BufferedReader(fr);
            String str = null;
            while ((str = br.readLine()) != null) {
                String[] strs = str.split("\t");
                map.put(strs[0], strs[1]);
            }
            br.close();
        } catch (IOException io) {
            io.printStackTrace();
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
    
    public static void main(String[] args) throws JsonSyntaxException, IOException {
        // get lists and maps
        getSentimentMap();
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
                String tmpText = twitterReduce.getText();
                String[] words = textSplit(tmpText);
                
                twitterReduce.setTwi_id(parts[0]);
                twitterReduce.setSentiment(getDensity(getCount(words), getScore(words)));
                twitterReduce.setText(wordsCensor(tmpText, twitterReduce.getUser_id()));
                
                // get output Json
                String json = gson.toJson(twitterReduce);

                sysout.print(json);
                sysout.print("\n");
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
