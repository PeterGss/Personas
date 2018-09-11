package com.pt.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopUserAgent {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("tool", "/wjh/useragent/tool.xml");
		conf.set("app", "/wjh/useragent/app.xml");
		conf.set("browser", "/wjh/useragent/browserUser.xml");
		conf.set("os", "/wjh/useragent/os.xml");
		Job job = Job.getInstance(conf, "useragent");
		job.setJarByClass(HadoopUserAgent.class);
		job.setMapperClass(UserAgentParseMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
