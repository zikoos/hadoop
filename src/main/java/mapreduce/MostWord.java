
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.text.wikipedia.XmlInputFormat;

public class MostWord {

	public static class FirstTitleLetterMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private static final String START_DOC = "<text xml:space=\"preserve\">";
		private static final String END_DOC = "</text>";
		private static final Pattern TITLE = Pattern
				.compile("<title>(.*)<\\/title>");
		private static final Pattern WORD = Pattern
				.compile("*/w");

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String articleXML = value.toString();

			String title = getTitle(articleXML);
			String document = getDocument(articleXML);
			String word=getWord(articleXML);

			if (title.length() > 0) {
				context.write(new Text(title.substring(0, 1)), new IntWritable(
						document.length()));
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
		
		private static ArrayList<String> getWord(CharSequence xml) {
			Matcher m = WORD.matcher(xml);
			ArrayList<String> listWord = new ArrayList<String>();
			while(m.find()){
				
				listWord.add(m.group(1));
			}
			return listWord;
		}


	}

	public static class DocumentLengthSumReducer extends
			Reducer<Text, IntWritable, Text, LongWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			long totalLength = 0;
			for (IntWritable documentLength : values) {
				totalLength += documentLength.get();
			}
			context.write(key, new LongWritable(totalLength));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		Job job = Job
				.getInstance(conf, "MostWord");
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
		job.setNumReduceTasks(12);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
