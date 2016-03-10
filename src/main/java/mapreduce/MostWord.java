
package mapreduce;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.tools.internal.xjc.util.ForkContentHandler;
import com.sun.tools.javac.code.Type.ForAll;

public class MostWord {

  // this is the name of the jar you compile, so hadoop has its fucntion hooks
  private final static String JAR_NAME = "wc.jar";   

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    // this is the name of the jar you compile, so hadoop has its fucntion hooks!
    private final static String JAR_NAME = "wc.jar";
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());

        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    //liste des 100 mots qui ont le max sum
    //ArrayList<Text> mylist = new ArrayList<Text>(100);
    TreeMap<Integer,Text> mylist= new TreeMap<>();
    
    int indicemin=0;

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
      Text conca =new Text(key+"|||"+result);
      
      if(mylist.size()>100){
    	  if(mylist.lastKey()<sum){
    		  mylist.remove(mylist.size()-1);
    	      mylist.put(sum, key);
    	  }
      }
      else{
    	  mylist.put(sum, key);
      }
    	  		
	}
    
    for(Entry<Integer,Text> entry : treeMap.desc){
    	context.write();    	
    }
    
    
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJar( JAR_NAME );
    job.setJarByClass(MostWord.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}