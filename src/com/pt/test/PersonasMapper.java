package com.pt.test;

import com.google.gson.Gson;
import com.useragentutils.UserAgent;
import com.useragentutils.UserAgentManager;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;


/**
 * Created by Shaon on 2018/8/24.
 */
public class PersonasMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final static Log log = LogFactory.getLog(PersonasMapper.class);
    //mac imei 正则
    Pattern macPattern = Pattern.compile("([0-9la-fA-F]{2})(([/\\s:|-]?+[0-9a-fA-F]{2}){5})");
    Pattern imeiPattern =  Pattern.compile("^(\\d{15}|\\d{17})$");
    String[] terminalfeatures ;
    Text outputKey = new Text();
    Text outputValue = new Text();
    //APPPROTO 要处理的协议
    String appproto;

    //浏览器 配置、应用 配置
    Map<String,AppFaker> APPMap = new HashMap<String,AppFaker>();
    Map<String,BrowerUserCharacter> browerMap = new HashMap<String,BrowerUserCharacter>();

    //浏览器 特征
    String browerUserSignField;
    //浏览器 提取用户特征
    List<BrowerUserCharacter> browerSigns = new ArrayList<BrowerUserCharacter>();
    // 要分析的字段， 目前只考虑 cookie 和 uri
    String browerField;
    //
    List<String> hostlist = new ArrayList<String>();
    //伪装浏览器的 应用
    String appNames[];
    List<AppFaker> appFakerList = new ArrayList<AppFaker>();
    String appUseragent[];
    String appHosts[];
    String appReferer[];
    String appCookieCharacter[];
    String appUriCharacter[];

    private UserAgentManager userAgentManager;
    //UserAgentManager userAgentManager = UserAgentManager.getInstance("F:\\BDB\\mr\\cpcenter\\Personas\\conf\\personas\\tool.xml",
           //"F:\\BDB\\mr\\cpcenter\\Personas\\conf\\personas\\app.xml", "F:\\BDB\\mr\\cpcenter\\Personas\\conf\\personas\\browserUser.xml", "F:\\BDB\\mr\\cpcenter\\Personas\\conf\\personas\\os.xml");


    protected void setup(Context context) throws IOException, InterruptedException {
        Loaddata(context);
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Configuration conf = context.getConfiguration();
        appproto = conf.get("appproto","HTTP");
        terminalfeatures = conf.get("terminal","").split(",");
        appNames = conf.get("apps").split(",",-1);
        //伪装成 浏览器的应用
        for (String app : appNames) {
             appUseragent = conf.get(app + ".useragent").split(",",-1);
             appHosts  = conf.get(app + ".hosts").split(",",-1);
             appReferer = conf.get(app + ".referer").split(",",-1);
             appCookieCharacter = conf.get(app + ".cookie.character").split(",",-1);
             appUriCharacter = conf.get(app + ".uri.character").split(",",-1);
            appFakerList.add(new AppFaker(app,appUseragent,appHosts,appReferer,appCookieCharacter,appUriCharacter));
        }
        //浏览器的用户特征
        String hosts[] = conf.get("brower.hosts").split(",",-1);
        hostlist = Arrays.asList(hosts);
        for (String host : hosts) {
            String  browerSign = conf.get(host + ".userCharacter");
            // * ^ : | . \ 需要前面佳\\转义
            for (String character : browerSign.split("\\.",-1)){
                String uriOrc = StringUtils.substringBefore(character,":");
                String []filed = StringUtils.substringBetween(character,":").split(",",-1);
                String split = StringUtils.substringAfterLast(character,":");
                browerMap.put(host,new BrowerUserCharacter(host,uriOrc,filed,split));
            }
        }
    }

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (value == null) {
            log.error("record is null!");
            return;
        }
        Gson gson = new Gson();
        Bean bean = gson.fromJson(value.toString(),Bean.class);
        String mac = parseTerminal(bean.Uri,terminalfeatures); //终端
        String imei = "";
        String useragent = "";
        //UserAgent userAgent = new UserAgent();
        //用户特征
        String userinfo ="";
        //过滤掉 不处理的数据
        if (!appproto.contains(bean.getAppProto()) || StringUtils.isEmpty(bean.getUserAgent())){
            return;
        }
        //1.识别 浏览器 工具 应用   分别处理
        useragent = bean.getUserAgent();
        //解析 userAgent
        //userAgent = UserAgent.parseUserAgentString(useragent);
        UserAgent userAgent = userAgentManager.parseUserAgent(useragent);

        String browerName = userAgent.getBrowserName();
        String toolname = userAgent.getToolName();
        String appName = userAgent.getAppName();
        //如果是 工具
        if (StringUtils.isNotEmpty(toolname)){
            outputKey.set(toolname);
        }
        //如果是浏览器 行为  输出 ip + 设备类型+操作系统 + 浏览器+浏览器版本 + TTL 作为一个终端的 key
        else if (StringUtils.isNotEmpty(browerName)){
            //判断是否是 应用伪装为浏览器
            for (AppFaker appFaker : appFakerList) {
                if (appFaker.getAppUseragent().equals(bean.UserAgent)){
                    if ((StringUtils.isNotEmpty(bean.Cookie) && bean.Cookie.contains(appFaker.appCookieCharacter[0]))
                            || (StringUtils.isNotEmpty(bean.Uri) && bean.Uri.contains(appFaker.appUriCharacter[0]))
                            || Arrays.asList(appFaker.appReferer).contains(bean.Referer)
                            || Arrays.asList(appFaker.appHosts).contains(bean.Host)){
                        //根据应用的特征 写
                        return;
                    }
                }
            }
            //map 提取用户特征
            if (browerMap.containsKey(bean.Host)){
                for (String field:browerMap.get(bean.Host).field){
                    String userField = "";
                    switch (browerMap.get(bean.Host).browerField){
                        case "Uri":
                            userField = bean.Uri;
                            break;
                        case "Cookie":
                            userField = bean.Cookie;
                            break;
                        case "SetCookie":
                            break;
                        case "LocalUri":
                            break;
                        default:
                            break;
                    }
                    if (StringUtils.isNotEmpty(userField) && userField.contains(field)) {
                        //前缀
                        String preUserinfo = "";
                        if (StringUtils.isNotEmpty(userinfo)){
                            preUserinfo = ",";
                        }
                        userinfo += preUserinfo + returnUserCharacter(userField, field, browerMap.get(bean.Host).fieldSplit, -1, "");
                    }
                }

            }
            //提取 用户特征
            for (BrowerUserCharacter browerSign : browerSigns) {
                if (hostlist.contains(bean.Host) && bean.Host.equals(browerSign.host)){
                    for (String field:browerSign.field){
                        String userField = "";
                        switch (browerSign.browerField){
                            case "Uri":
                                userField = bean.Uri;
                                break;
                            case "Cookie":
                                userField = bean.Cookie;
                                break;
                            case "SetCookie":
                                break;
                            case "LocalUri":
                                break;
                            default:
                                break;
                        }
                        if (StringUtils.isNotEmpty(userField) && userField.contains(field)) {
                            //前缀
                            String preUserinfo = "";
                            if (StringUtils.isNotEmpty(userinfo)){
                                preUserinfo = ",";
                            }
                            userinfo += preUserinfo + returnUserCharacter(userField, field, browerSign.fieldSplit, -1, "");
                        }
                    }
                }
            }
            outputKey.set(userAgent.getOSName() + "," +userAgent.getBrowserName() + "," +userAgent.getBrowserVersion()
                    + "," + bean.getSrcIP() + "," + bean.getTTL());
        }
        // 应用 也需要提取 用户特征
        else if (StringUtils.isNotEmpty(appName)){
            outputKey.set(userAgent.getOSName()+ "," + userAgent.getAppName()
                    + "," + bean.getSrcIP() + "," + bean.getTTL());
        }
        //2. 识别提取用户特征，这个要根据 xml配置 针对性提取

        //若是浏览器 无mac 等终端信息的  操作系统 + 设备类型设备类型 +浏览器 +浏览器版本+应用+TTL
        outputValue.set(userinfo + "\t" + bean.Host +"\t" + bean.UserAgent);
        //3终端融合 有mac的以mac为key，没mac 用终端融合,value :srcip 终端 应用（浏览器） 用户  行为 rectime
        //if (StringUtils.isNotEmpty(userinfo)) {
            context.write(outputKey, outputValue);
        //}
        // 若存在 mac等 终端信息 mac 也输出
        if(StringUtils.isNotEmpty(mac)){
            for (String macstr :  mac.split("&",-1)) {
                if (StringUtils.isNotEmpty(macstr)){
                    if ((macstr.contains("mac=")
                            && macPattern.matcher(macstr.replace("mac=","")).matches())
                            ||(macstr.contains("imei=")
                               && imeiPattern.matcher(macstr.replace("imei=","")).matches())) {
                        context.write(new Text(macstr), outputValue);
                    }
                }
            }
        }
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
    /**
     *根据分隔符  提取 用户特征
     * @param str
     * @param charcterField
     * @param split
     * @param index
     * @param indexSplit
     * @return
     */
    public String returnUserCharacter(String str,String charcterField,String split,int index,String indexSplit){
        String character = "";
            if (str.contains(str) && str.contains(charcterField)) {
                int startIndex = str.indexOf(charcterField);
                int endIndex = str.length() -1;
                //后面还有其他参数的时候 按照& 来截取
                if (str.substring(startIndex + 1).contains(split)) {
                    endIndex = str.indexOf(split, startIndex + 1);
                }
                String terminalStr = str.substring(startIndex, endIndex);
                if (StringUtils.isEmpty(indexSplit) && StringUtils.isNotEmpty(terminalStr)) {
                    character += terminalStr;
                }else if(StringUtils.isNotEmpty(indexSplit) ){
                    character = terminalStr.split(indexSplit,-1)[index];
                }
            }
        return character;
    }

    //解析 提取 终端特征
    public String parseTerminal(String str,String []terminalfeatures) {
        String terminal = "";
        for (String terminalfeature : terminalfeatures) {
            if (str.contains(terminalfeature)) {
                int startIndex = str.indexOf(terminalfeature);
                int endIndex = str.length() -1;
                //后面还有其他参数的时候 按照& 来截取
                if (str.substring(startIndex + 1).contains("&")) {
                    endIndex = str.indexOf("&", startIndex + 1);
             }
                String terminalStr = str.substring(startIndex, endIndex);
                if (StringUtils.isNotEmpty(terminalStr)) {
                    terminal += terminalStr;
                }
            }
        }
        return terminal;
    }
}
