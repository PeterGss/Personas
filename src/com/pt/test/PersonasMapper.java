package com.pt.test;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.useragentutils.UserAgent;
import com.useragentutils.UserAgentManager;
import com.xmlutils.Application;
import com.xmlutils.Browser;
import com.xmlutils.ReadXml;
import com.xmlutils.User;
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
import java.util.*;
import java.util.regex.Pattern;


/**
 * Created by Shaon on 2018/8/24.
 */
public class PersonasMapper extends Mapper<LongWritable, Text, Text, Text> {

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
    }

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (value == null) {
            log.error("record is null!");
            return;
        }
        StringBuilder valuesb = new StringBuilder();
        Gson gson = new Gson();

        Bean bean = gson.fromJson(value.toString(), Bean.class);
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
        UserAgent userAgent = userAgentManager.parseUserAgent(useragent);
        String browserName = userAgent.getBrowserName();
        String toolname = userAgent.getToolName();
        String appName = userAgent.getAppName();
        //如果是 工具
        if (StringUtils.isNotEmpty(toolname)){
            outputKey.set(toolname + "\t" + bean.getSrcIP());
        }
        //如果是浏览器 行为  输出 ip + 设备类型+操作系统 + 浏览器+浏览器版本 + TTL 作为一个终端的 key
        else if (StringUtils.isNotEmpty(browserName)){
            //判断是否是 应用伪装为浏览器
           String appFakerName = isAppFaker(browserVersion.get(browserName),bean,userAgent);
            if (StringUtils.isNotEmpty(appFakerName)){
                //是应用
                outputKey.set(bean.getSrcIP() + "," + userAgent.getOSName()+ "," + userAgent.getAppName()
                        + "," +  bean.getTTL());
                // 提取用户信息
                userinfo += getAppCharacter(appFakerName,bean) ;
                valuesb.append(bean.getSrcIP() +"\t" + userinfo + "\t" + userAgent.getOSName()
                        + "\t" + userAgent.getAppName() + "\t" + bean.getHost() + bean.RecTime);
                //提取终端信息
            }else {
                //是浏览器
                    //提取用户信息  查看  host中 是否包含 browserma 中配的 host
                    for (String s : browserMap.keySet()) {
                        if(bean.Host.contains(s)) {
                            userinfo += browserCharacter(s, bean);
                            break;
                        }
                    }
                //终端
                mac +=  parseTerminal(bean,Arrays.asList(terminalfeatures));
                imei +=  parseTerminal(bean,Arrays.asList(terminalfeatures));
                outputKey.set( bean.getSrcIP() + "," + userAgent.getDeviceType() + "," + userAgent.getOSName() + "," +userAgent.getBrowserName() + "," +userAgent.getBrowserVersion()
                        + ","  + bean.getTTL());
                valuesb.append(bean.getSrcIP() +"\t" + userinfo + "\t" + userAgent.getOSName()
                        + "\t" + browserName + "\t" + bean.Host + "\t" + bean.RecTime);
            }

        }// 应用 提取 用户特征
        else if (StringUtils.isNotEmpty(appName)){
            if (APPMap.containsKey(appName)) {
                outputKey.set(bean.getSrcIP() + "," + userAgent.getOSName() + "," + userAgent.getAppName()
                        + "," +  bean.getTTL());

                Application app = APPMap.get(appName);
                userinfo += getAppCharacter(appName,bean) ;
                valuesb.append(bean.getSrcIP() +"\t" + userinfo + "\t" + userAgent.getOSName()
                        + "\t" + userAgent.getAppName() + "\t" + bean.getHost() + bean.RecTime);

                //提取 终端 信息
                List<String> macs = app.getMac();
                if (macs.size() > 0){
                    mac = parseTerminal(bean,macs);
                }
                List<String> imeis = app.getImei();
                if (imeis.size() > 0) {
                    imei = parseTerminal(bean, imeis);
                }
            }
        } else{
            mos.write(new Text(bean.UserAgent + "\t" + bean.getHost()), new Text(),"noUserAgent/noUserAgent.bcp");
            moscounter.increment(1);
        }
        //2. 识别提取用户特征，这个要根据 xml配置 针对性提取
        //若是浏览器 无mac 等终端信息的  操作系统 + 设备类型设备类型 +浏览器 +浏览器版本+应用+TTL
                //3终端融合 有mac的以mac为key，没mac 用终端融合,value :srcip 终端 应用（浏览器） 用户  行为 rectime
       if (valuesb.length() !=0){
           outputValue.set(valuesb.toString());
       }else {
           outputValue.set(bean.SrcIP +"\t" + userinfo + "\t" + bean.Host +"\t" + bean.UserAgent + bean.RecTime);
       }

        if (StringUtils.isNotEmpty(userinfo)) {
            context.write(outputKey, outputValue);
        }
        // 若存在 mac、imei等 终端信息 mac 也输出
        if(StringUtils.isNotEmpty(mac)){
            mac = mac.replace("&","");
            if ((mac.contains("mac=")
                    && macPattern.matcher(mac.replace("mac=","")).matches())) {
                context.write(new Text(mac), outputValue);
            }
        }
        if(StringUtils.isNotEmpty(imei)){
            imei = imei.replace("&","");
            if ((imei.contains("imei=")
                    && imeiPattern.matcher(imei.replace("imei=","")).matches())) {
                context.write(new Text(imei), outputValue);
            }
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
        Path browserVersion = new Path(conf.get("browserVersion"));
        Path  browserApp = new Path(conf.get("browserApp"));
        Path appFeature = new Path(conf.get("appFeature"));
        Path browserFeature = new Path(conf.get("browserFeature"));
        FileSystem tooFlileSystem = FileSystem.get(conf);
        InputStream browserVersionStream = tooFlileSystem.open(browserVersion);
        InputStream browserAppSystem = tooFlileSystem.open(browserApp);
        InputStream appFeatureSystem = tooFlileSystem.open(appFeature);
        InputStream browserFeatureSystem = tooFlileSystem.open(browserFeature);

        ReadXml readXml = new ReadXml(browserVersionStream,
                browserAppSystem,
                appFeatureSystem,
                browserFeatureSystem);
        return  readXml;
    }

    //获取 浏览器的用户特征
    public String browserCharacter(String host, Bean bean){
        String userinfo ="";
        Browser browser = browserMap.get(host);
        List<User> userlist = browser.getUser();
        for (User user : userlist) {
            List<String> uservalue = user.getValue();
            String split = user.getSplit();
            String position = user.getPosition();
            userinfo += returnUserCharacter(bean,user);
        }
        return userinfo;
    }
    //获取应用 的用户特征
    public  String getAppCharacter(String appName, Bean bean){
        String userinfo ="";
        Application app = APPMap.get(appName);
        List<User> userlist = app.getUser();
        for (User user : userlist) {
            List<String> uservlue = user.getValue();
            String split = user.getSplit();
            String position = user.getPosition();
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
    public String returnUserCharacter(Bean bean, User user) {
        String type = user.getType();
            if ( StringUtils.isEmpty(type) ) {
                return "";
            }
            switch (type){
                case "1":
                    return type1( bean, user);
                case "2":
                    return type1( bean, user);
                case "3":
                    return type3( bean, user);
                case "4":
                    break;
                default:
                    return "";
            }
        return "";
    }

    public String type1(Bean bean, User user){
        List<String> uservalue = user.getValue();
        String indexSplit = user.getSplit();
        String index = user.getPosition();
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
                    if (characterStrs.length >= Integer.parseInt(index)) {
                        character += characterStrs[Integer.parseInt(index)];
                    }
                }
            }
        }
        return character;
    }

    //json 数据
    public String type3(Bean bean, User user) {
        List<String> uservalue = user.getValue();
            String indexSplit = user.getSplit();
        String index = user.getPosition();
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
                        if (characterStrs.length >= Integer.parseInt(index)) {
                            character += characterStrs[Integer.parseInt(index)];
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
    public String isAppFaker(String browserName, Bean bean, UserAgent userAgent){
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
    public String parseTerminal(Bean bean, List<String> terminalfeatures) {
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
