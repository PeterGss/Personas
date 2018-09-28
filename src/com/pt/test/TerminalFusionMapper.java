package com.pt.test;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.pt.util.MLogger;
import com.pt.util.Utils;
import com.useragentutils.UserAgent;
import com.useragentutils.UserAgentManager;
import com.xmlutils.Application;
import com.xmlutils.Browser;
import com.xmlutils.ReadXml;
import com.xmlutils.User;
import org.apache.commons.collections.map.HashedMap;
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
import org.apache.hadoop.mapreduce.Reducer;
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
public class TerminalFusionMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final static Log log = LogFactory.getLog(TerminalFusionMapper.class);
    private MultipleOutputs<Text,Text> mos;
    //工具 频率阈值判断
    float toolThreshold = 100;
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

    private HashMap<String, String> terminalAnalysisHostMap = new HashMap();
    private HashMap<String, String> terminalAnalysisAppMap = new HashMap();
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
//        List<User> browuserlist = new ArrayList<User>();
//        List<String> browhostList = Arrays.asList("btrace.qq.com");
//        List<String> browmacList = Arrays.asList("");
//        List<String> browimeiList = Arrays.asList("");
//        List<String> browvalueList = Arrays.asList("Uri:qq");
//        browuserlist.add(new User("2",browvalueList,"",""));
// browserMap.put("qq.com",new Browser("qq.com","扣扣",browmacList,browimeiList,browuserlist));
        //伪装成 浏览器的应用
        appFaker = readXml.getBrowserAppMap();
        //appFaker.put("Internet Explorer 6",Arrays.asList("暴风影音，爱奇艺，QQ，YY语音"));
        APPMap = readXml.getAppFeatureMap();
//        List<User> userlist = new ArrayList<User>();
//        List<String> hostList = Arrays.asList("pan.baidu.com");
//        List<String> macList = Arrays.asList("Cookie:mac");
//        List<String> imeiList = Arrays.asList("");
//        List<String> versionList = Arrays.asList("URI:version");
//        List<String> osList = Arrays.asList("URI:os");
//        List<String> valueList = Arrays.asList("Cookie:BAIDUID");
//        userlist.add(new User("2",valueList,"",""));
//        APPMap.put("百度云管家",new Application("百度云管家",hostList,macList,versionList,osList,imeiList,userlist));

        //浏览器的版本
        browserVersion = readXml.getBrowserVersionMap();

        terminalAnalysisHostMap = readXml.getTerminalAnalysisHostMap();
        terminalAnalysisAppMap = readXml.getTerminalAnalysisAppMap();
    }

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (value == null) {
            log.error("record is null!");
            return;
        }
        StringBuilder valuesb = new StringBuilder();
        Gson gson = new Gson();
        String str = value.toString();
        try {
            str = Utils.getURLDecoderString(str);
        }catch (IllegalArgumentException e){
            if (str.contains("%u")){
                if (str.contains("%u")){
                    String unicode =  decodeUnicode(str.replace("%u","\\u"));
                    if (!Strings.isNullOrEmpty(unicode)){
                        str = unicode;
                    }
                }
            }
        }
        String[] strs = str.split(PerConstants.SEPARATOR,-1);
        //uri转码
        Bean bean = new Bean();
        if (strs.length >10) {
            bean = new Bean(strs[3], strs[4], strs[5], decode(strs[6]), decode(strs[7]), strs[8], strs[9]);
        }else
        {
            MLogger.info("value.toString():" + value.toString());
            MLogger.info("length:" + strs.length);
        }
        String mac = "";
        String imei = "";
        String useragent = "";
        //用户特征
        String userinfo ="";
        //过滤掉 不处理的数据
        if (!appproto.contains(bean.getAppProto()) || StringUtils.isEmpty(bean.getUserAgent())){
            return;
        }
        //1.识别 浏览器 工具 应用   分别处理
        useragent = bean.getUserAgent();
        //解析 userAgent
        useragent = bean.getUserAgent();
        //解析 userAgent
        UserAgent userAgent = new UserAgent("");
        try {
            userAgent = userAgentManager.parseUserAgent(useragent);
        }catch (IllegalArgumentException e){
            MLogger.warn(e + e.getMessage());
        }

        String browserName = userAgent.getBrowserName();
        String toolname = userAgent.getToolName();
        String appName = userAgent.getAppName();

        Map<String,String> map = new HashMap<String,String>();

            //如果是 工具
            if (StringUtils.isNotEmpty(toolname)) {
                outputKey.set(toolname + PerConstants.SEPARATOR + bean.getSrcIP());
            }
            else if (StringUtils.isNotEmpty(browserName)) {
                if (terminalAnalysisHostMap.containsKey(bean.Host)) {
                    context.write(new Text(bean.SrcIP + PerConstants.SEPARATOR  + terminalAnalysisHostMap.get(bean.Host))
                            , new Text(PerConstants.BROWSER + PerConstants.SEPARATOR + bean.sepString() + PerConstants.SEPARATOR + browserVersion.get(browserName)));
                }
            }//应用
            else if (StringUtils.isNotEmpty(appName)){
                if (terminalAnalysisAppMap.containsKey(appName)) {
                    context.write(new Text(bean.SrcIP + PerConstants.SEPARATOR  +terminalAnalysisAppMap.get(appName))
                            , new Text(PerConstants.APP +  PerConstants.SEPARATOR + bean.sepString()+  PerConstants.SEPARATOR + appName));
                }
            }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    public static String decode(String str){
        String str2 = str ;
        try {
            str2 = Utils.getURLDecoderString(str);
        }catch (IllegalArgumentException e){
            if (str2.contains("%u")){
                String unicode =  decodeUnicode(str.replace("%u","\\u"));
                if (!Strings.isNullOrEmpty(unicode)){
                    str2 = unicode;
                }
            }
        }
        return str2;
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

    //获取 浏览器的用户特征
    public String browserCharacter(String host,Bean bean){
        String userinfo ="";
        Browser browser = browserMap.get(host);
        List<User> userlist = browser.getUser();
        for (User user : userlist) {
            List<String> uservalue = user.getValue();
            String split = user.getSplit();
            int position = user.getPosition();
            userinfo += returnUserCharacter(bean,user);
        }
        return userinfo;
    }
    //获取应用 的用户特征
    public  String getAppCharacter(String appName,Bean bean){
        String userinfo ="";
        Application app = APPMap.get(appName);
        List<User> userlist = app.getUser();
        for (User user : userlist) {
            List<String> uservlue = user.getValue();
            String split = user.getSplit();
            int position = user.getPosition();
            userinfo += returnUserCharacter(bean,user);
        }
        return userinfo;

    }
    /**
     *提取 用户特征
     * @param bean
     * @param user
     * @return
     */
    public String returnUserCharacter(Bean bean,User user) {
        int type = user.getType();
            if ( type ==-1 ) {
                return "";
            }
            switch (type){
                case 1:
                    return type1( bean, user);
                case 2:
                    return type1( bean, user);
                case 3:
                    return type3( bean, user);
                case 4:
                    break;
                default:
                    return "";
            }
        return "";
    }

    public String type1(Bean bean,User user){
        List<String> uservalue = user.getValue();
        String indexSplit = user.getSplit();
        int index = user.getPosition();
        String character = "";
        for (String value : uservalue) {
            if (StringUtils.isEmpty(value)) {
                break;
            }
            String valuearr[] = value.split(":", -1);
            String str = needParseStr(valuearr[0], bean);
            String fieldfeature = valuearr[1];
            if (!fieldfeature.endsWith("=")){
                fieldfeature += "=";
            }
            String split = "";
            if (valuearr[0].equalsIgnoreCase("Uri")) {
                split = uriSplit;
            } else if (valuearr[0].equalsIgnoreCase("Cookie")) {
                split = cookieSplit;
            }
            if (str.contains(fieldfeature)) {
                int startIndex = str.indexOf(fieldfeature);
                int endIndex = str.length() -1;
                //后面还有其他参数的时候 按照split 来截取
                if (str.substring(startIndex + 1).contains(split)) {
                    endIndex = str.indexOf(split, startIndex + 1);
                }
                String characterStr = str.substring(startIndex, endIndex);
                if (StringUtils.isNotEmpty(characterStr) && StringUtils.isEmpty(indexSplit)) {
                    character += characterStr;
                }else if (StringUtils.isNotEmpty(characterStr) && StringUtils.isEmpty(indexSplit)){
                    String characterStrs[] = characterStr.split(indexSplit,-1);
                    if (characterStrs.length >= index) {
                        character += characterStrs[index];
                    }
                }
            }
        }
        return character;
    }

    //json 数据
    public String type3(Bean bean,User user) {
        List<String> uservalue = user.getValue();
            String indexSplit = user.getSplit();
        int index = user.getPosition();
        String character = "";
        for (String value : uservalue) {
            if (StringUtils.isEmpty(value)) {
                break;
            }
            String valuearr[] = value.split(":", -1);
            String str = needParseStr(valuearr[0], bean);
            String fieldfeature = valuearr[1];
            if (valuearr.length == 2){
                if (!fieldfeature.endsWith("=")) {
                    fieldfeature += "=";
                }
                String split = "";
                if (valuearr[0].equalsIgnoreCase("Uri")) {
                    split = uriSplit;
                } else if (valuearr[0].equalsIgnoreCase("Cookie")) {
                    split = cookieSplit;
                }
                if (str.contains(fieldfeature)) {
                    int startIndex = str.indexOf(fieldfeature);
                    int endIndex = str.length() - 1;
                    //后面还有其他参数的时候 按照split 来截取
                    if (str.substring(startIndex + 1).contains(split)) {
                        endIndex = str.indexOf(split, startIndex + 1);
                    }
                    String characterStr = str.substring(startIndex, endIndex);
                    if (StringUtils.isNotEmpty(characterStr) && StringUtils.isEmpty(indexSplit)) {
                        character += characterStr;
                    } else if (StringUtils.isNotEmpty(characterStr) && StringUtils.isEmpty(indexSplit)) {
                        String characterStrs[] = characterStr.split(indexSplit, -1);
                        if (characterStrs.length >= index) {
                            character += characterStrs[index];
                        }
                    }
                }
            }else{//需要解析json
                String jsonvalue = valuearr[2];
                character += parseJson(jsonvalue);
            }
    }
        return character;
    }

    //判断是否是 应用伪装为浏览器
    public String isAppFaker(String browserName,Bean bean,UserAgent userAgent){
        boolean isApp =false;
        if (null != appFaker.get(browserName) && appFaker.get(browserName).size() > 0){
            for (String appFaker : appFaker.get(browserName)) {
                Application appfaker = APPMap.get(appFaker);
                if (null == appfaker){
                    return "";
                }
                for (String appVersion : appfaker.getVersion()) {
                    if (StringUtils.isNotEmpty(appVersion)){
                        String []version = appVersion.split(":",-1);
                        String userField = needParseStr(version[0],bean);
                        if (version[0].equalsIgnoreCase("Uri")){
                            if (bean.getUri().contains(version[1])){
                                isApp = true;
                            }
                        }else if (version[0].equalsIgnoreCase("Cookie")){
                            if (bean.getCookie().contains(version[1])){
                                isApp = true;
                            }
                        }
                        /*//这种方法得 完全匹配
                        if (isApp && appfaker.getHost().contains(bean.Host) ){
                            return appfaker.getName();
                        }*/

                        for (String hosts : appfaker.getHost()) {
                            if (bean.Host.contains(hosts)){
                                return appfaker.getName();
                            }
                        }
                    }
                }
            }
        }
        return  "";
    }
    //解析 提取 终端特征
    public String parseTerminal(Bean bean,List<String> terminalfeatures) {
        String terminal = "";
        String split = "";
        for (String ter : terminalfeatures) {
            if (StringUtils.isEmpty(ter)){
                break;
            }
            String terminalFeatures[] = ter.split(":",-1);
            String str = needParseStr(terminalFeatures[0],bean);
            String terminalfeature = terminalFeatures[1];
            if (terminalFeatures[0].equalsIgnoreCase("Uri")){
                split = uriSplit;
            }else if (terminalFeatures[0].equalsIgnoreCase("Cookie")){
                split = cookieSplit;
            }
            if (str.contains(terminalfeature)) {
                int startIndex = str.indexOf(terminalfeature);
                int endIndex = str.length() -1;
                //后面还有其他参数的时候 按照& 来截取
                if (str.substring(startIndex + 1).contains(split)) {
                    endIndex = str.indexOf(split, startIndex + 1);
                }
                String terminalStr = str.substring(startIndex, endIndex);
                if (StringUtils.isNotEmpty(terminalStr)) {
                    terminal += terminalStr;
                }
            }
        }
        return terminal;
    }

    //返回需要 分析的字段值
    public String needParseStr(String str,Bean bean){
        String userField = "";
        switch (str){
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
        return userField;
    }

    // 获取 数据中，对应字段的值
    public String getFieldValue(String str,String field,String split){
        String terminal = "";
        if (str.contains(field)) {
            int startIndex = str.indexOf(field);
            int endIndex = str.length() -1;
            //后面还有其他参数的时候 按照& 来截取
            if (str.substring(startIndex + 1).contains(split)) {
                endIndex = str.indexOf(split, startIndex + 1);
            }
            String terminalStr = str.substring(startIndex, endIndex);
            if (StringUtils.isNotEmpty(terminalStr)) {
                terminal += terminalStr;
            }
        }
        return terminal;
    }


}
