package com.pt.test;

import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by Shaon on 2018/9/10.
 */
public class CleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    String appproto = "HTTP";
    String LogType = "http_request";
    MultipleOutputs<Text,NullWritable> mos ;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<Text,NullWritable>(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Gson gson = new Gson();
        Bean bean = gson.fromJson(value.toString(),Bean.class);
        //过滤掉 不处理的数据
        if (appproto.equalsIgnoreCase(bean.getAppProto()) && StringUtils.isNotEmpty(bean.getUserAgent())
            && LogType.equalsIgnoreCase(bean.getLogType())){
            context.write(value, NullWritable.get());
            mos.write(new Text(bean.toString()),NullWritable.get(),"/bean/");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
