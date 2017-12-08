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

public class DataExtractProb {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(),","); //Splits the input based on spaces and newlines
	  int c=0;
	  String batsman="",bowler="";
	  int runs=-2;
      while (itr.hasMoreTokens()) {//Extracting the fields from the dataset
	    String token=itr.nextToken();
	    if(c==0&&(token.equals("version")||token.equals("info")))
			return;
		if(c==4)
			batsman=token.trim();
		else if(c==6)
			bowler=token.trim();
		else if(c==7)
			runs=Integer.parseInt(token.trim());
		else if(c==9&&(token.trim().charAt(0)>='a'&&token.trim().charAt(0)<='z'&&token.trim().charAt(0)!='r'))
			runs=-1;
		c++;
      }
      context.write(new Text(batsman+","+bowler), new IntWritable(runs));
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,Text> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double one = 0, two=0,three=0,four=0,dot=0,six=0,balls=0, wickets = 0;
      for (IntWritable val : values) {
		int temp=val.get();
		if(temp==0)//Counting the number of 0s, 1s, 2s, 3s, 4s, 6s and Wickets by a batsman against a particular bowler
			dot++;
		else if(temp==1)
			one++;
		else if(temp==2)
			two++;
		else if(temp==3)
			three++;
		else if(temp==4)
			four++;
		else if(temp==6)
			six++;
		else if(temp==-1)
			wickets++;
		balls++;
      }
	  context.write(key, new Text(Double.toString(dot/balls)+","+Double.toString(one/balls)+","+Double.toString(two/balls)+","+Double.toString(three/balls)+","+Double.toString(four/balls)+","+Double.toString(six/balls)+","+Double.toString(1-(wickets/balls))));//Output - Player to Player Probabilities
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "IPL Data Extraction");
    job.setJarByClass(DataExtractProb.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
