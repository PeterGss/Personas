package com.pt.test;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.useragentutils.UserAgent;
import com.useragentutils.UserAgentManager;

public class UserAgentParseMapper extends Mapper<Object, Text, Text, Text> {

	private UserAgentManager userAgentManager;
	private Text word = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		UserAgent userAgent = userAgentManager.parseUserAgent(value.toString());
		word.set(userAgent.toString());
		context.write(value, word);

	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Loaddata(context);
		super.setup(context);
	}

	private void Loaddata(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		String tool = conf.get("tool");
		Path toolPath = new Path(tool);
		FileSystem tooFlileSystem = toolPath.getFileSystem(conf);
		InputStream toolIn = tooFlileSystem.open(toolPath);
		String app = conf.get("app");
		Path appPath = new Path(app);
		FileSystem appFileSystem = appPath.getFileSystem(conf);
		InputStream appIn = appFileSystem.open(appPath);
		String browser = conf.get("browser");
		Path browserPath = new Path(browser);
		FileSystem browserFileSystem = browserPath.getFileSystem(conf);
		InputStream browserIn = browserFileSystem.open(browserPath);

		String os = conf.get("os");
		Path osPath = new Path(os);
		FileSystem osFileSystem = osPath.getFileSystem(conf);
		InputStream osIn = osFileSystem.open(osPath);

		userAgentManager = UserAgentManager.getInstance(toolIn, appIn, browserIn, osIn);

		
	}

}
