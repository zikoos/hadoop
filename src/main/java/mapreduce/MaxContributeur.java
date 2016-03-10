

	
	
	package mapreduce;

	import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
	import java.util.regex.Pattern;

	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
	import org.apache.mahout.text.wikipedia.XmlInputFormat;

import com.sun.tools.javac.util.List;

	public class MaxContributeur {

		public static class FirstTitleLetterMapper extends
				Mapper<Object, Text, Text, IntWritable> {

			private static final String START_DOC = "<text xml:space=\"preserve\">";
			private static final String END_DOC = "</text>";
			private static final Pattern TITLE = Pattern
					.compile("<title>(.*)<\\/title>");
			private static final Pattern Contributeur = Pattern
					.compile("<username><(.*)<\\/username>");
			private final static IntWritable one = new IntWritable(1);

			public void map(Object key, Text value, Context context)
					throws IOException, InterruptedException {

				String articleXML = value.toString();

				String title = getTitle(articleXML);
				ArrayList<String> listContributor = getContributeur(articleXML);
				String document = getDocument(articleXML);
				
				for (String string : listContributor) {
					if (string.length() > 0) {
						context.write(new Text(string), one);	
					}
				}
	

			}

			private static String getDocument(String xml) {
				int start = xml.indexOf(START_DOC) + START_DOC.length();
				int end = xml.indexOf(END_DOC, start);
				return start < end ? xml.substring(start, end) : "";
			}

			private static String getTitle(CharSequence xml) {
				Matcher m = TITLE.matcher(xml);
				return m.find() ? m.group(1) : "";
			}
			private static ArrayList<String> getContributeur(CharSequence xml) {
				Matcher m = Contributeur.matcher(xml);
				ArrayList<String> listContributor = new ArrayList<String>();
				while(m.find()){
					listContributor.add(m.group(1));
				}
				return listContributor;
			}

		}

		public static class DocumentLengthSumReducer extends
				Reducer<Text, IntWritable, Text, LongWritable> {
					
			public void reduce(Text key, Iterable<IntWritable> values,
					Context context) throws IOException, InterruptedException {

				long sum = 0;
				for (IntWritable val : values) {
					sum += val.get();
				}
				context.write(key, new LongWritable(sum));
			}
		}	

		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration();
			conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
			conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

			Job job = Job
					.getInstance(conf, "MaxContributeur_zak-phil");
			job.setJarByClass(WikiFirstTitleLetterDocumentLengthSum.class);

			// Input / Mapper
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(XmlInputFormat.class);
			job.setMapperClass(FirstTitleLetterMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			// Output / Reducer
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			job.setReducerClass(DocumentLengthSumReducer.class);
			job.setNumReduceTasks(20);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}