import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import java.io.IOException;
import java.util.*;

public class DoubleSort {

	public static class MyCompartor extends WritableComparator {
		MyCompartor() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			String[] atokens = a.toString().split(",");
			String[] btokens = b.toString().split(",");
			int amonth = Integer.parseInt(atokens[0]);
			int bmonth = Integer.parseInt(btokens[0]);
			int adate = Integer.parseInt(atokens[1]);
			int bdate = Integer.parseInt(btokens[1]);
			float atemperature = Float.parseFloat(atokens[2]);
			float btemperature = Float.parseFloat(btokens[2]);

			if (amonth != bmonth)
				return Integer.compare(amonth, bmonth);
			else if (adate != bdate)
				return Integer.compare(adate, bdate);
			else
				return Float.compare(btemperature, atemperature);	 // Switch a,b (ascending order)
		}
	}

	public static class MyGrouper extends WritableComparator {
		MyGrouper() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			String[] atokens = a.toString().split(",");
			String[] btokens = b.toString().split(",");
			int amonth = Integer.parseInt(atokens[0]);
			int bmonth = Integer.parseInt(btokens[0]);
			int adate = Integer.parseInt(atokens[1]);
			int bdate = Integer.parseInt(btokens[1]);

			if (amonth != bmonth)
				return Integer.compare(amonth, bmonth);
			else
				return Integer.compare(adate, bdate);
		}
	}

	public static class MyMapper extends Mapper<Object, Text, Text, Text>{

		private Text outKey = new Text();
		private Text outVal = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer strtoken = new StringTokenizer(value.toString(), "\":-, ");		// Input cut
			ArrayList<String> tokens = new ArrayList<String>();								// Token result
			while(st.hasMoreTokens()){
				tokens.add(strtoken.nextToken());
			}
			// value = "\"2013-01-01 00:20:00\",48.2" -> tokens = [2013,01,01,00,20,00,48.2]
			if(tokens.size() >= 7) {
				outKey.set(tokens.get(1)+ "," + tokens.get(2) + "," + tokens.get(6));		// Message needed
				outVal.set(value.toString());												// OUtput value (initial message)
				context.write(outKey, outVal);												// Write into context
			}
		}
	}

	public static class MyPartitioner extends HashPartitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String[] tokens = key.toString().split(",");
			String dateKey = tokens[0] + tokens[1];
			return (dateKey.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text date = new Text();
			Text temperature = new Text();

			for (Text val : values) {
				String[] tokens = key.toString().split(",");
				date.set(String.valueOf(Integer.parseInt(tokens[0])) + "/" + String.valueOf(Integer.parseInt(tokens[1])));
				temperature.set(temperature.toString() + " " +  tokens[2]);
			} 
			context.write(date, temperature);
		}
	}

	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Double Sort");
		
		job.setJarByClass(DoubleSort.class);
		job.setMapperClass(MyMapper.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setSortComparatorClass(MyCompartor.class);
		job.setGroupingComparatorClass(MyGrouper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	   }
	   
}
