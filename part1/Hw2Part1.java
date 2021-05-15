import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Hw2Part1 {
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
    private final static IntWritable one = new IntWritable(1);
    private Text keyOut = new Text();
    private Text valueOut = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      while (itr.hasMoreTokens()) {
        String str = itr.nextToken();
        String[] strArr = str.split(" ");
        if(strArr.length != 3){
          continue;
        }
        String source = strArr[0];
        String destination = strArr[1];
        String timestr = strArr[2];

        Pattern pattern = Pattern.compile("^[0-9]*\\.?[0-9]+$");
        if(!pattern.matcher(timestr).matches()){
          continue;
        }
        double time = Double.valueOf(timestr);
        String result_key = source + " " + destination;
        String result_value = one + " " + time;
        keyOut.set(result_key);
        valueOut.set(result_value);
        context.write(keyOut, valueOut);
      }
    }
  }
  
  public static class IntSumCombiner extends Reducer<Text,Text,Text,Text> {
    private Text valueOut = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
      int counts = 0;
      double times = 0;
      double avgTime;
      for(Text value : values){
        String record = value.toString();
        StringTokenizer oneRecord = new StringTokenizer(record);
        int count = Integer.valueOf(oneRecord.nextToken());
        double time = Double.valueOf(oneRecord.nextToken());
        counts = counts + count;
        times = times + time;
      }
      avgTime = times/counts;
      String strValue = Integer.toString(counts) + " " + Double.toString(avgTime);
      valueOut.set(strValue);
      context.write(key, valueOut);
    }
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,Text> {

    private Text result_key= new Text();
    private Text result_value= new Text();
    

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int counts = 0;
      double times = 0;
      double avgTime;
      for(Text value : values){
        String record = value.toString();
        StringTokenizer oneRecord = new StringTokenizer(record);
        int count = Integer.valueOf(oneRecord.nextToken());
        double time = Double.valueOf(oneRecord.nextToken());
        counts = counts + count;
        times = times + time;
      }
      avgTime = times/counts;
      String strValue = Integer.toString(counts) + " " + Double.toString(avgTime);
      result_key.set(key);
      result_value.set(strValue);
      context.write(result_key, result_value);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "word count");

    job.setJarByClass(Hw2Part1.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // add the input paths as given by command line
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }

    // add the output path as given by the command line
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

