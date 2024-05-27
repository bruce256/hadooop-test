package com.midea.hadoop.mapreduce.sort;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 如果需要本地运行，则需要导入这个包
 *
 * <dependency>
 * <groupId>org.apache.hadoop</groupId>
 * <artifactId>hadoop-mapreduce-client-common</artifactId>
 * <version>${v-hadoop}</version>
 * </dependency>
 *
 * @author lvsheng
 * @date 2019-09-01
 **/
@Slf4j
public class Sort {
	
	public static class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		
		private final static IntWritable one         = new IntWritable(1);
		private              IntWritable intWritable = new IntWritable();
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			intWritable.set(Integer.valueOf(value.toString()));
			context.write(intWritable, one);
		}
	}
	
	
	public static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			context.write(key, SortMapper.one);
		}
	}
	
	public static void main(String[] args) throws Exception {
		try {
			long start = System.currentTimeMillis();
			
			// 1. 获取配置文件对象，获取job对象实例
			Configuration conf = new Configuration();
			Job           job  = Job.getInstance(conf, "word count");
			
			// 2. 指定程序jar的本地路径
			job.setJarByClass(Sort.class);
			
			//  3. 指定Mapper/Reducer类
			job.setMapperClass(SortMapper.class);
			job.setReducerClass(SortReducer.class);

//			job.setNumReduceTasks(27);
//			job.setPartitionerClass(MyPartitioner.class);
			
			// 5. 指定最终输出的kv数据类型
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);
			
			// 6. 指定job处理的原始数据路径
//			FileInputFormat.addInputPath(job, new Path("/Users/LvSheng/work/data/aclImdb_v1_train_datasets/pos"));
//			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileInputFormat.addInputPath(job, new Path("/Users/LvSheng/code/github/hadoop-test/input"));
			
			//  7. 指定job输出结果路径
//			FileOutputFormat.setOutputPath(job, new Path("/Users/LvSheng/work/data/aclImdb_v1_train_datasets/data_out"));
//			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path("/Users/LvSheng/code/github/hadoop-test/output"));
			
			//  8. 提交作业
			job.waitForCompletion(true);
			
			log.info("time cost ： " + (System.currentTimeMillis() - start) / 1000 + " s");
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		} catch (IllegalStateException e) {
			log.error(e.getMessage(), e);
		} catch (IllegalArgumentException e) {
			log.error(e.getMessage(), e);
		} catch (InterruptedException e) {
			log.error(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			log.error(e.getMessage(), e);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}
}