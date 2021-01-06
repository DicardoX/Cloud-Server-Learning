import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
public class DoubleSort {

	public static class DoubleSortMapper extends Mapper<Object, Text, Text, Text>{

		private Text outkey = new Text();
		private Text outval = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer st = new StringTokenizer(value.toString(), "\":-, ");
			ArrayList<String> tokens = new ArrayList<String>();
			while(st.hasMoreTokens()){
				tokens.add(st.nextToken());
			}
			/* e.g. if value = "\"2013-01-01 00:20:00\",48.2", then 
			* tokens = [2013,01,01,00,20,00,48.2] length = 7
			*/
			if(tokens.size() >= 7) {
				outkey.set(tokens.get(1)+ "," + tokens.get(2) + "," + tokens.get(6));
				outval.set(value.toString());
				context.write(outkey, outval);
			}
		}
	}

	public static class DoubleSortPartitioner extends HashPartitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String[] tokens = key.toString().split(",");
			String dateKey = tokens[0] + tokens[1];
			return (dateKey.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static class DoubleSortCompartor extends WritableComparator {
		DoubleSortCompartor() {
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
			else // switch a,b because temperature is in ascending order
				return Float.compare(btemperature, atemperature);
		}
	}

	public static class DoubleSortGrouper extends WritableComparator {
		DoubleSortGrouper() {
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

	public static class DoubleSortReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			Text date = new Text();
			Text tem = new Text();

			for (Text val : values) {
				String[] tokens = key.toString().split(",");
				date.set(String.valueOf(Integer.parseInt(tokens[0])) + "/" + String.valueOf(Integer.parseInt(tokens[1])));
				tem.set(tem.toString() + " " +  tokens[2]);
			} 
			context.write(date, tem);

		}
	}

	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Double Sort");
	
		job.setJarByClass(DoubleSort.class);
		job.setMapperClass(DoubleSortMapper.class);
		job.setPartitionerClass(DoubleSortPartitioner.class);
		job.setSortComparatorClass(DoubleSortCompartor.class);
		job.setGroupingComparatorClass(DoubleSortGrouper.class);
		job.setReducerClass(DoubleSortReducer.class);
		
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
