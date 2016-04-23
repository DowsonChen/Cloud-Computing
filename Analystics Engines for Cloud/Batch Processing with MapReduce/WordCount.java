import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class Map extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Remove <ref> </ref> and so on
        String temp1 = line.replaceAll("</ref>|<ref[^>]*>", " ");
        // Remove [ and ]
        String temp2 = temp1.replaceAll("\\[|\\]"," ");
        // Remove http https ftp
        String temp3 = temp2.replaceAll("(https?|ftp):\\/\\/[^\\s/$.?#][^\\s]*", " ");
        // Remove all ' except within a word
        String temp4 = temp3.replaceAll("\'+[^A-Za-z]|(?<![a-zA-Z])'", " ");
        // Remove all non character except '
        String temp5 = temp4.replaceAll("[^A-Za-z']", " ");
        // Change all white space to " " and all word to lower case
        String temp6 = temp5.replaceAll("\\s+", " ").toLowerCase();

        // Save all words in current line to an array
        StringTokenizer tokenizer = new StringTokenizer(temp6);
        String[] list = new String[tokenizer.countTokens()];
        for (int i = 0; i < list.length; i++) {
            list[i] = tokenizer.nextToken();
        }
        
        // Get 1 - 5 gram
        for (int i = 1; i < 6; i++) {
            for (int j = 0; j < list.length + 1 - i; j++) {
                StringBuilder sb = new StringBuilder("");
                for (int k = 0; k < i; k++) {
                    if (k == 0) {
                        sb.append(list[j]);
                    } else {
                        sb.append(" ").append(list[j + k]);
                    }
                }
                word.set(sb.toString().trim());
                context.write(word, one);
            }
        }
    }
  }

  public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "dowson");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
