package com.pt.appfaker;

import com.google.gson.Gson;
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
 * Created by Shaon on 2018/9/18.
 */
public class ToolMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final static Log log = LogFactory.getLog(PersonasMapper.class);
    private MultipleOutputs<Text,Text> mos;
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
    public ToolMapper() {
        super();
    }

    @Override
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

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value == null) {
            log.error("record is null!");
            return;
        }
        StringBuilder valuesb = new StringBuilder();
        Gson gson = new Gson();
        Bean bean = new Bean ();
        //uri转码
        try {
             bean = gson.fromJson(Utils.getURLDecoderString(value.toString().replace("\t","")),Bean.class);
        }catch (Exception e){
            bean = gson.fromJson(value.toString(),Bean.class);
            MLogger.warn("bean from json getURLDecoder exception:"+e.getMessage() + e);
        }
               String useragent = "";
        //用户特征
        //过滤掉 不处理的数据
        if (!appproto.equalsIgnoreCase(bean.getAppProto()) || StringUtils.isEmpty(bean.getUserAgent())){
            return;
        }
        //1.识别 浏览器 工具 应用   分别处理
        useragent = bean.getUserAgent();
        //解析 userAgent
        UserAgent userAgent = userAgentManager.parseUserAgent(useragent);
        String browserName = userAgent.getBrowserName();
        String toolname = userAgent.getToolName();
        String appName = userAgent.getAppName();
        //如果是 工具
        if (StringUtils.isNotEmpty(toolname)){
            context.write(new Text(PerConstants.FREQUENCYTOOL + PerConstants.SEPARATOR + bean.SrcIP + PerConstants.SEPARATOR + bean.DstIP),new Text(bean.sepString()));
        }
        //如果是浏览器 行为  输出 ip + 设备类型+操作系统 + 浏览器+浏览器版本 + TTL 作为一个终端的 key
        else{
            //计算频率
            context.write(new Text(PerConstants.FREQUENCY + PerConstants.SEPARATOR + bean.SrcIP + PerConstants.SEPARATOR + bean.DstIP), new Text(bean.sepString()));
          }
        }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
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

}
