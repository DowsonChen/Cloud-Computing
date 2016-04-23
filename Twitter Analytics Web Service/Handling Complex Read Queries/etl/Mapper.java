import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import java.util.*;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

public class Mapper {
    // Create Twitter data class
    public static class twitter_data {
        public class User {
            @SerializedName("id")
            public long id;
        }
        public String str_id;
        @SerializedName("id")
        public long tweet_id;
        @SerializedName("id_str")
        public String id_str;
        @SerializedName("user")
        public User user;
        @SerializedName("created_at")
        public String created_time;           
        @SerializedName("text")
        public String text;
        @SerializedName("lang")
        public String lang;
        public boolean check_valid() {
            try {
                // Delete data without en field
                if (lang == null || !lang.equals("en"))
                    return false;
                if (user.id == 0L || created_time.length() == 0 || text.length() == 0)
                    return false;
                if (tweet_id == 0L && id_str.length() == 0) 
                    return false;
                else if (tweet_id == 0L && id_str.length() != 0)
                    str_id = id_str;
                else if (tweet_id != 0L && id_str.length() == 0)
                    str_id = Long.toString(tweet_id);
                else {
                    if (Long.toString(tweet_id).equals(id_str) == false)
                        return false;
                    else
                        str_id = id_str;
                }
                return true;
            }
            catch(Exception e) {
                return false;
            }
        }
    }

    public static class twitter_reduce {
        public long user_id;
        public String created_time;
        public String text;
        // Transfor data format
        public twitter_reduce (long user_id, String created_time, String text) throws Exception {
            this.user_id = user_id;
            SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
            SimpleDateFormat new_dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            new_dateFormat.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
            Date date = dateFormat.parse(created_time);
            this.created_time = new_dateFormat.format(date);
            this.text = text;
        }
    }

    public static void main(String[] args) throws Exception {   
        BufferedReader sysin = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        PrintStream sysout = new PrintStream(System.out, true, "UTF-8");
        String line = "";
        // Transfer JSON format
        while ((line = sysin.readLine()) != null) {
            Gson gson = new Gson();
            twitter_data data = gson.fromJson(line, twitter_data.class);
            if (data.check_valid()) {
                twitter_reduce output = new twitter_reduce(data.user.id, data.created_time, data.text);
                String gson_output = gson.toJson(output);
                sysout.println(data.str_id + "\t" + gson_output);
            }
        }
    }
}
