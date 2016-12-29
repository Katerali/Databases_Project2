package ua.edu.ucu.ads;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Arrays;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer;


class Mapper extends TableMapper<Text, IntWritable> {

	private final IntWritable ONE = new IntWritable(1);
	
	private Text text = new Text();

   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
   		byte[] patient_key = Arrays.copyOfRange(row.get(), 0, 16);
   		byte[] type = value.getValue(Bytes.toBytes("medical_records"), Bytes.toBytes("type"));
   		String val = new String();
   		for (int i = 0; i < patient_key.length; i++) {
   			val += new Integer(patient_key[i]);
   		}
        text.set(val + " - " + new String(type));
        context.write(text, ONE);
   	}
}


class JobReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int i = 0;
		for (IntWritable val : values) {
			i += val.get();
		}
		context.write(key, new IntWritable(i));
	}
}


public class MapReduceJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Patient counter");
        job.setJarByClass(MapReduceJob.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(JobReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.setNumReduceTasks(1);

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		scan.addFamily("medical_records".getBytes());
		
		TableMapReduceUtil.initTableMapperJob("medical_records", scan, Mapper.class, Text.class, IntWritable.class, job);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}