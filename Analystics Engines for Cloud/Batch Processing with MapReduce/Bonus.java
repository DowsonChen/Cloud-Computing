import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Bonus {

    public static class Map extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();
        private Text autoWord = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null || line.length() == 0) {
                return;
            }
            
            String[] parts = line.split("\t");
            String phrase = parts[0];
            String count = parts[1];
            // Kick out 2 - 5 gram
            String[] words = phrase.split(" ");
            if (words.length > 1) {
                return;
            }
            
            autoWord.set(phrase + " " + count);
            StringBuilder currKey = new StringBuilder();
            for (int i = 0; i < phrase.length() - 1; i++) {
                char ch = phrase.charAt(i);
                currKey.append(ch);
                word.set(String.valueOf(currKey));
                context.write(word, autoWord);
            }
        }
      }
    
     public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable> {
        private static int RANK = 5;
        
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            String lastWord = "";
            int number = 0;
            int sum = 0;
            // creat a map to sort.
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            for (Text value : values) {
                String str = value.toString();
                String count = "";
                // find the position of the " "
                // String[] info = str.split("\\s+");
                int endIndex = str.lastIndexOf(" ");
                lastWord = str.substring(0, endIndex).trim();
                count = str.substring(endIndex).trim();
                number = Integer.parseInt(count);
                map.put(lastWord, number);
                sum += number;
            }
            // sort the ket-value by count number
            List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>> (map.entrySet());
            Collections.sort(list, new Comparator <Entry<String, Integer>>() {
                    public int compare(Entry < String, Integer > h1,
                        Entry < String, Integer > h2) {
                        int result = (Integer) h2.getValue() - (Integer) h1.getValue();
                        if (result == 0) {
                            return (h1.getKey().toString()).compareTo((h2.getKey().toString()));
                        }
                        return result;
                    }
            });
            
            int totalSize = 0;
            String wordNext = "";
            double wordCount = 0;
            double probability = 0;
            // get the size of the result which output.
            if (list.size() >= RANK) {
                totalSize = RANK;
            } else {
                totalSize = list.size();
            }

            Put put = new Put(Bytes.toBytes(key.toString()));

            int i = 0;
            while (i < totalSize) {
                wordNext = list.get(i).getKey();
                wordCount = list.get(i).getValue();
                probability = wordCount * 100.0 / sum;
                put.add(Bytes.toBytes("data"),
                        Bytes.toBytes(wordNext),
                        Bytes.toBytes(String.valueOf(probability)));
                i++;
            }
            if (!put.isEmpty()) {
                context.write(new ImmutableBytesWritable(key.getBytes()), put);    
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = new Job(conf, "dowson");
        job.setJarByClass(Bonus.class);

        TableMapReduceUtil.initTableReducerJob("bonus", Reduce.class, job);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
