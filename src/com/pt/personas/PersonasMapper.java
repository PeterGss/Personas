package com.pt.personas;

import com.google.gson.Gson;
import com.pt.util.MLogger;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


/**
 * Created by Shaon on 2018/8/24.
 */
public class PersonasMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final static Log log = LogFactory.getLog(PersonasMapper.class);

    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Configuration conf = context.getConfiguration();

    }

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (value == null) {
            log.error("record is null!");
            return;
        }
        Gson gson = new Gson();
        Bean bean = new Bean();
        bean = gson.fromJson(value.toString(),Bean.class);

        //识别 浏览器 工具 应用
        if (StringUtils.isNotEmpty(bean.getUserAgent())){
            String useragent = "";
            useragent = bean.getUserAgent();
        }
        System.out.println(bean.toString());

        //终端融合

    }

}
