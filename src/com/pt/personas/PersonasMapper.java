package com.pt.personas;

import com.google.gson.Gson;
import com.pt.util.MLogger;
import eu.bitwalker.useragentutils.UserAgent;
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
    Text outputKey = new Text();
    Text outputValue = new Text();


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
        String mac = ""; //终端
        UserAgent userAgent =null;
        //识别 浏览器 工具 应用
        if (StringUtils.isNotEmpty(bean.getUserAgent())){
            String useragent = "";
            useragent = bean.getUserAgent();
            userAgent = UserAgent.parseUserAgentString(useragent);
            MLogger.info(userAgent.toString());
        }
        outputKey.set(bean.sepString());
        outputValue.set
                (bean.getSrcIP() + "\t"
                        + mac + "\t"
                        + userAgent.getApplication());

        //终端融合 有mac的以mac为key，没mac 用终端融合,value :srcip 终端 应用（浏览器） 用户  行为 rectime
        context.write(outputKey,outputValue);

    }

    public String uerAgentToString(){

        return "";
    }

}
