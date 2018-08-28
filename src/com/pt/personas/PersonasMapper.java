package com.pt.personas;

import com.google.gson.Gson;
import com.pt.util.MLogger;
import eu.bitwalker.useragentutils.*;
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
        String useragent = "";
        UserAgent userAgent = new UserAgent();
        //1.识别 浏览器 工具 应用   分别处理
        if (StringUtils.isNotEmpty(bean.getUserAgent())){
            useragent = bean.getUserAgent();
            userAgent = UserAgent.parseUserAgentString(useragent);
           // MLogger.info(userAgent.toString());
        }
        //2. 识别提取用户特征，这个要根据 xml配置 针对性提取

        //无mac 等终端信息的  操作系统 + 设备类型设备类型 +浏览器 +浏览器版本+应用+TTL
        outputKey.set(uerAgentToString(useragent) + "\t" + bean.getTTL());
        bean.setUserAgent("");
        outputValue.set(bean.sepString());
        //终端融合 有mac的以mac为key，没mac 用终端融合,value :srcip 终端 应用（浏览器） 用户  行为 rectime
        context.write(outputKey,outputValue);

        // 若存在 mac等 终端信息 mac 也输出
        if(StringUtils.isNotEmpty(mac)){
            context.write(new Text(mac),outputValue);
        }

    }

    public String uerAgentToString(String str){
        UserAgent userAgent = UserAgent.parseUserAgentString(str);
        OperatingSystem os = userAgent.getOperatingSystem();
        String osName = os.getName();
        String deType = os.getDeviceType().getName();
        String osManufacturer = os.getManufacturer().getName();

        Browser bwr = userAgent.getBrowser();
        String bwrType = bwr.getBrowserType().getName();
        String bwrName = bwr.getName();
        String bwrEngine = bwr.getRenderingEngine().name();
        String bwrManufacturer = bwr.getManufacturer().getName();
        Version version = userAgent.getBrowserVersion();
        String bwrVersion = null;
        String bwrmajorVersion = null;
        String bwrminorVersion = null;
        if (version != null) {
            bwrVersion = version.getVersion();
            bwrmajorVersion = version.getMajorVersion();
            bwrminorVersion = version.getMinorVersion();
        }
        Application app = userAgent.getApplication();
        String appName = app.getName();
        String appType = app.getApplicationType().getName();
        String appManufacturer = app.getManufacturer().getName();
        return osName + "\t"
                + deType + "\t"
                + bwr + "\t"
                + bwrVersion + "\t"
                + app + "\t"
                + appName;
    }

}
