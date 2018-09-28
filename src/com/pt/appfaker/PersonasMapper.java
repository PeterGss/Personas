package com.pt.appfaker;

import com.google.common.base.Strings;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.pt.util.MLogger;
import com.pt.util.Utils;
import com.useragentutils.UserAgent;
import com.useragentutils.UserAgentManager;
import com.xmlutils.Application;
import com.xmlutils.Browser;
import com.xmlutils.ReadXml;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * Created by Shaon on 2018/8/24.
 */
public class PersonasMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final static Log log = LogFactory.getLog(PersonasMapper.class);
    private MultipleOutputs<Text,Text> mos;
    //工具 频率阈值判断
    float toolThreshold = 10000;
    //mac imei 正则
    Pattern macPattern ;
    Pattern imeiPattern ;
    String[] terminalfeatures ;
    Text outputKey = new Text();
    Text outputValue = new Text();
    //APPPROTO 要处理的协议
    String appproto;
    Counter moscounter;
    String uriSplit = "";
    String cookieSplit = "";

    //应用 配置
    Map<String,Application> APPMap = new HashMap<String,Application>();
    //浏览器 配置
    Map<String,Browser> browserMap = new HashMap<String,Browser>();
    //浏览器名称 包含的应用
    Map<String,List<String>> appFaker = new HashMap<String,List<String>>();
    //浏览器版本信息
    Map<String,String> browserVersion = new HashMap<String,String>();
    //浏览器 特征
    String browserUserSignField;
    //浏览器 提取用户特征
    String browserField;
    List<String> hostlist = new ArrayList<String>();
    //伪装浏览器的 应用
    String appNames[];
    private UserAgentManager userAgentManager;

    protected void setup(Context context) throws IOException, InterruptedException {
        Loaddata(context);
        mos = new MultipleOutputs<Text,Text>(context);
        moscounter = context.getCounter(CounterEnum.MOSCOUNTER);
        //解析 配置
        ReadXml readXml =getReadXml(context);
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Configuration conf = context.getConfiguration();
        appproto = conf.get("appproto","HTTP");
        terminalfeatures = conf.get("terminal","").split(",");
        appNames = conf.get("apps").split(",",-1);
        //uri 解析
        uriSplit = conf.get("uriSplit","&");
        cookieSplit = conf.get("cookieSplit",";");
        //mac imei 正则编译
        String macRegex = conf.get("macRegex","([0-9la-fA-F]{2})(([/\\s:|-]?+[0-9a-fA-F]{2}){5})");
        String imeiRegex = conf.get("imeiRegex","^(\\d{15}|\\d{17})$");

        macPattern = Pattern.compile(macRegex);
        imeiPattern = Pattern.compile(imeiRegex);
        //浏览器
        browserMap = readXml.getBrowserFeatureMap();
        //伪装成 浏览器的应用
        appFaker = readXml.getBrowserAppMap();
        APPMap = readXml.getAppFeatureMap();
        //浏览器的版本
        browserVersion = readXml.getBrowserVersionMap();
    }

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (value == null) {
            log.error("record is null!");
            return;
        }
        StringBuilder valuesb = new StringBuilder();
        String str = value.toString();
        try {
            str = Utils.getURLDecoderString(str);
        }catch (IllegalArgumentException e){
            if (str.contains("%u")){
                String unicode =  decodeUnicode(str.replace("%u","\\u"));
                if (!Strings.isNullOrEmpty(unicode)){
                    str = unicode;
                }
            }
        }
        String[] strs = str.split(PerConstants.SEPARATOR,-1);

        Bean bean = new Bean();
        float frequency = 0;
        if (strs.length >10) {
            bean = new Bean(strs[3], strs[4], strs[5], strs[6], strs[7], strs[8], strs[9]);
            frequency = Float.parseFloat(strs[10]);
        }else
        {
            MLogger.info("value.toString():" + value.toString());
            MLogger.info("length:" + strs.length);
        }
        String useragent = "";
        //过滤掉 不处理的数据
        if (!appproto.contains(bean.getAppProto()) || StringUtils.isEmpty(bean.getUserAgent())){
            return;
        }
        //1.识别 浏览器 工具 应用   分别处理
        useragent = bean.getUserAgent();
        //解析 userAgent
        UserAgent userAgent = userAgentManager.parseUserAgent(useragent);
        String browserName = userAgent.getBrowserName();
        String toolname = userAgent.getToolName();
        String appName = userAgent.getAppName();
        String browserversion = userAgent.getBrowserVersion();

        String os = userAgent.getOSName();
        String devicetype = userAgent.getDeviceType();

        if (frequency < toolThreshold) {
            //如果是 工具
            if (StringUtils.isNotEmpty(toolname)) {
                context.write(new Text(PerConstants.FREQUENCYTOOL + PerConstants.SEPARATOR + bean.SrcIP + PerConstants.SEPARATOR + bean.DstIP), new Text(bean.sepString()));
                outputKey.set(toolname + PerConstants.SEPARATOR + bean.getSrcIP());
            }
            //如果是浏览器行为 输出 useragent 标识符\tuseragent\t浏览器 valuebean
            else if (StringUtils.isNotEmpty(browserName)) {
                context.write(new Text(PerConstants.USERAGENT + PerConstants.SEPARATOR + bean.UserAgent
                                + PerConstants.SEPARATOR + browserVersion.get(browserName)+ PerConstants.SEPARATOR +
                        os + PerConstants.COMMA + devicetype + PerConstants.COMMA + browserName + PerConstants.COMMA + browserversion)
                        , new Text(bean.sepString()));
            }//应用
            else if (StringUtils.isNotEmpty(appName)){
                ResultInfo info = getUserInfo(bean,appName);
                context.write(new Text(PerConstants.APP + PerConstants.SEPARATOR + info.toAppResultInfo()),new Text());
            }
            //识别不出的 useragent 输出出来
            else{
                mos.write(new Text(bean.UserAgent + PerConstants.SEPARATOR + bean.getHost()), new Text(),"noUserAgent/noUserAgent.bcp");
                moscounter.increment(1);
            }
        }else {
            // tool
            context.write(new Text(PerConstants.FREQUENCYTOOL + PerConstants.SEPARATOR + bean.SrcIP + PerConstants.SEPARATOR + bean.DstIP), new Text(bean.sepString()));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    //解析json 数据
    static private String parseJson(String str) throws JsonSyntaxException{
        JsonParser parse =new JsonParser();
        return parse.parse(str).getAsString();
    }
    //读取 配置文件  加载数据
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
    //
    private ReadXml getReadXml(Context context)throws IOException{
        Configuration conf = context.getConfiguration();
        Path browserVersionMap = new Path(conf.get("browserVersion"));
        Path  browserApp = new Path(conf.get("browserApp"));
        Path appFeature = new Path(conf.get("appFeature"));
        Path browserFeature = new Path(conf.get("browserFeature"));
        Path dataRelation = new Path(conf.get("dataRelation"));
        Path osUnify = new Path(conf.get("osUnify"));
        Path terminalAnalysis = new Path(conf.get("terminalAnalysis"));
        FileSystem toolFileSystem = FileSystem.get(conf);
        InputStream browserVersionMapStream = toolFileSystem.open(browserVersionMap);
        InputStream browserAppSystem = toolFileSystem.open(browserApp);
        InputStream appFeatureSystem = toolFileSystem.open(appFeature);
        InputStream browserFeatureSystem = toolFileSystem.open(browserFeature);
        InputStream dataRelationSystem = toolFileSystem.open(dataRelation);
        InputStream osUnifySystem = toolFileSystem.open(osUnify);
        InputStream terminalAnalysisSystem = toolFileSystem.open(terminalAnalysis);

        ReadXml readXml = new ReadXml(browserVersionMapStream,
                browserAppSystem,
                appFeatureSystem,
                browserFeatureSystem,
                dataRelationSystem,
                osUnifySystem,
                terminalAnalysisSystem);
        return  readXml;
    }

    // 提取应用中的用户信息
    private ResultInfo getUserInfo(Bean bean,String APPNAME){
        ResultInfo info = new ResultInfo();
        // 模拟数据 提取结果数据

        String HOST = "";
        String WEBSITE = "";
        String MAC = "";
        String IMEI = "";
        String APPVERSION = "";
        String OS = "";
        String USERINFO = "";

        HashMap<String, String> cookieMap = Discern.parseCookie(bean.Cookie);
        HashMap<String, String> uriMap = Discern.parseURI(bean.Uri);
        info.setSrcip(bean.SrcIP);
        if (!Strings.isNullOrEmpty(APPNAME)) {
            Application application = APPMap.get(APPNAME);
            info.setAppname(APPNAME);
            if (application != null) {
                // 获取mac
                MAC = Discern.hashSetToStringFormat(Discern.getMac(cookieMap, uriMap, application.getMac()));
                info.setMac(MAC);
                // 获取imei
                IMEI = Discern.hashSetToStringFormat(Discern.getImei(cookieMap, uriMap, application.getImei()));
                info.setImei(IMEI);
                // 获取应用版本
                APPVERSION = Discern.hashSetToStringFormat(Discern.getVersion(cookieMap, uriMap, application.getVersion()));
                info.setAppversion(APPVERSION);
                // 获取os
                OS = Discern.hashSetToStringFormat(Discern.getOs(cookieMap, uriMap, application.getOs()));
                info.setOs(OS);
                // 获取用户
                USERINFO = Discern.hashSetToStringFormat(Discern.getUser(cookieMap, uriMap, application.getUser()));
                info.setUserinfo(USERINFO);
            }
        }else{
            for (Map.Entry<String, Browser> entry : browserMap.entrySet()) {
                if (bean.getHost().contains(entry.getKey())) {
                    HOST = entry.getKey();
                    break;
                }
            }
            Browser browser = browserMap.get(HOST);
            if (browser != null) {
                // 访问网址
                WEBSITE = browser.getProduct();
                info.setWebsite(WEBSITE);
                // 获取mac
                MAC = Discern.hashSetToStringFormat(Discern.getMac(cookieMap, uriMap, browser.getMac()));
                info.setMac(MAC);
                // 获取imei
                IMEI = Discern.hashSetToStringFormat(Discern.getImei(cookieMap, uriMap, browser.getImei()));
                info.setImei(IMEI);
                // 获取用户
                USERINFO = Discern.hashSetToStringFormat(Discern.getUser(cookieMap, uriMap, browser.getUser()));
                info.setUserinfo(USERINFO);
                // 获取应用版本

            }
        }
        return info;
    }

    //unicode 转码
    public static String decodeUnicode(final String dataStr) {
        int start = 0;
        int end = 0;
        final StringBuffer buffer = new StringBuffer();
        while (start > -1) {
            end = dataStr.indexOf("\\u", start + 2);
            String charStr = "";
            if (end == -1) {
                charStr = dataStr.substring(start + 2, dataStr.length());
            } else {
                charStr = dataStr.substring(start + 2, end);
            }
            char letter = 0;
            try {
                letter = (char) Integer.parseInt(charStr, 16); // 16进制parse整形字符串。
            }catch (Exception e){
                return "";
            }
            buffer.append(new Character(letter).toString());
            start = end;
        }
        return buffer.toString(); //                buffer.append(new Character(letter).toString());                start = end;            }            return buffer.toString();

    }
}
