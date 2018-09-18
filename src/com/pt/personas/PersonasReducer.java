package com.pt.personas;

import com.google.common.base.Strings;
import com.useragentutils.UserAgent;
import com.useragentutils.UserAgentManager;
import com.xmlutils.Application;
import com.xmlutils.Browser;
import com.xmlutils.ReadXml;
import com.xmlutils.datarelationvo.FieldVO;
import com.xmlutils.datarelationvo.RelationVO;
import org.apache.commons.collections.list.TreeList;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.codehaus.groovy.ast.GenericsType;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by Shaon on 2018/8/24.
 */
public class PersonasReducer extends Reducer<Text, Text, Text, Text> {

    private UserAgentManager userAgentManager;
    private MultipleOutputs<Text,Text> mos;
    Counter moscounter;
    //判断 是否是伪装成浏览器的应用阈值
    float hostThreshold;
    //工具 频率阈值判断
    float toolThreshold;
    //计算频率的 时间区域
    Long frequencytimeZone = 10000l ;

    //captime ds cnt
    Long captime = new Date().getTime()/1000;
    //http
    String ds;
    int cnt = 1;

    StringBuilder sb = new StringBuilder();

    Set<String> outputSet = new HashSet<String>();

    //应用 配置
    Map<String,Application> APPMap = new HashMap<String,Application>();
    //浏览器 配置
    Map<String,Browser> browserMap = new HashMap<String,Browser>();
    //浏览器名称 包含的应用
    Map<String,List<String>> browAppFakerMap = new HashMap<String,List<String>>();
    //浏览器版本信息
    Map<String,String> browserVersionMap = new HashMap<String,String>();
    //关系提取 数据关联
    List<RelationVO> dataRelationList = new ArrayList<RelationVO>();

    Text outputValue;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        mos = new MultipleOutputs<Text,Text>(context);
        ds = conf.get("datasource","HTTP");
        captime = new Date().getTime()/1000;
        outputValue = new Text(captime + PerConstants.SEPARATOR +
                            ds + PerConstants.SEPARATOR + cnt);
        hostThreshold = conf.getFloat(PerConstants.HOSTTHRESHOLD,0.5f);
        Loaddata(context);
        ReadXml readXml =getReadXml(context);

        //频率 时间 区域
        frequencytimeZone = conf.getLong("frequencytimezone",10000l);
        //伪装成 浏览器的应用 浏览器名称 包含的应用
        browAppFakerMap = readXml.getBrowserAppMap();
        //浏览器
        browserMap = readXml.getBrowserFeatureMap();
        //浏览器的版本
        browserVersionMap = readXml.getBrowserVersionMap();
        APPMap = readXml.getAppFeatureMap();
        //关系提取
        dataRelationList = readXml.getDataRelationList();


        moscounter = context.getCounter(CounterEnum.MOSCOUNTER);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //合并数据
        String keys[] = key.toString().split(PerConstants.SEPARATOR,-1);
        List<String> valueList = new ArrayList<String>();
        List<String> hostList = new ArrayList<String>();
        List<String> rectimeList = new ArrayList<String>();
        float frequency;
        for (Text value : values) {
            String valueStr = value.toString();
            String[] valueStrs = valueStr.split(PerConstants.SEPARATOR,-1);
            if (!Strings.isNullOrEmpty(valueStrs[3])){
                hostList.add(valueStrs[3]);
            }
            if (!Strings.isNullOrEmpty(valueStrs[8])){
                rectimeList.add(valueStrs[8]);
            }
            if (!Strings.isNullOrEmpty(valueStr)){
                valueList.add(valueStr);
            }

        }
        Collections.sort(rectimeList);
        String appFaker ="";
        switch (keys[0]){
            // 工具类计算频率
            case PerConstants.FREQUENCY:
               if (!Strings.isNullOrEmpty(keys[1]) && !Strings.isNullOrEmpty(keys[2]) ) {
                    frequency = calcuFrequrncy(rectimeList);
                   for (String s : valueList) {
                       context.write(new Text(key.toString() + s), new Text(Float.toString(frequency)));
                   }
                }
                break;
            // 工具类计算频率
            case PerConstants.FREQUENCYTOOL:
                frequency = calcuFrequrncy(rectimeList);
                for (String s : valueList) {
                    context.write(new Text(key.toString() + s), new Text(Float.toString(frequency)));
                }
                break;
            // 识别伪装成浏览器的应用
            case PerConstants.USERAGENT:
                //解析 userAgent
                UserAgent userAgent = userAgentManager.parseUserAgent(keys[1]);
                 appFaker = appFaker(keys[2],valueList);
                if (Strings.isNullOrEmpty(appFaker)){
                    context.write(key,new Text(appFaker));
                }else {
                    for (String s : valueList) {
                        context.write(new Text(s), new Text(appFaker));
                    }
                }
                break;
            default:
                break;
        }
        //为空 说明未找到其伪装的应用，暂时可以当做 浏览器 处理

        ResultInfo info = new ResultInfo();
        info.setMac("4437E686FE9C");
        info.setSrcip("10.10.10.90");
        info.setAppname("应用");
        info.setBrowser("测试浏览器");
        info.setCombine("组合");
        info.setUserinfo("user");
        info.setWebsite("website");
        Set<String> set = new HashSet<String>();
        set = dataRelationExtract(info);
        for (String s : set) {
            outputSet.add(s);
        }



    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String filename = "NDEXLEVEL_1_1_RELATIONATTRIBUTE_" + captime+"_1.bcp";
        int count = 1;
        for (String s : outputSet) {
            mos.write(new Text(s),new Text(outputValue),"datarelation/"+filename);
        }
        mos.close();
    }

    //计算频率 s设定一个时间范围 超出这个时间范围的数据不计算频率，
    // 一个list 可以化成多个符合时间范围的 list 分别求频率，最后取平均值
    //目前单位 暂取 条/秒
    private float calcuFrequrncy(List<String>  strList){
        //用来存放时间范围内连续的数据
        List<Double> frequencylist = new ArrayList<Double>();
        if (strList.size() <=1){
            return 1l;
        }else{
            while (strList.size() >=2) {
                List<Double> rectimezone = new ArrayList<Double>();
                int count =0;
                while (strList.size() >=2 && Long.parseLong(strList.get(1)) - Long.parseLong(strList.get(0)) <= frequencytimeZone) {
                    if (count == 0) {
                        rectimezone.add(Double.parseDouble(strList.get(0)));
                    }
                    rectimezone.add(Double.parseDouble(strList.get(1)));
                    strList.remove(0);
                    count ++;
                }
                //小于一 说明无连续时间段的数据，我们默认 频率为1
                if (rectimezone.size() <= 1){
                    frequencylist.add(1.0);
                    strList.remove(0);
                }else{
                    Collections.sort(rectimezone);
                    double timeDifference = (rectimezone.get(rectimezone.size() -1) - rectimezone.get(0));
                    frequencylist.add(rectimezone.size()*1000/ (timeDifference == 0 ? 1 : timeDifference));
                }
            }
            double sum = 0l;
            for (Double aLong : frequencylist) {
                sum += aLong;
            }
            if (sum == 0){
                return 1;
            }else {
                return (float)Math.round((sum/frequencylist.size() * 100))/100;
            }
        }

    }

    //判断是不是 伪装成浏览器的应用
    private String appFaker(String browserName,List<String> strList){
        //计算比例用
        int count = 0 ;
        int listsize = strList.size();
        boolean hasAppVersion = false;
        if (null !=(browAppFakerMap.get(browserName)) && browAppFakerMap.get(browserName).size() > 0) {
            // 浏览器名字 对应的 伪装app
            for (String appFaker : browAppFakerMap.get(browserName)) {
                Application fakerApp = APPMap.get(appFaker);
                if (null == fakerApp) {
                    return "";
                }
                for (String str : strList) {
                    String[] strs = str.split(PerConstants.SEPARATOR, -1);
                    //统计 host 个数
                    for (String appHost : fakerApp.getHost()) {
                        String host = strs[3];
                        if (host.contains(appHost)) {
                            count++;
                        //有应用特征的 提取应用特征，无应用特征的 只需要判断阈值就可以
                        if (null != fakerApp.getVersion() && fakerApp.getVersion().size()>0) {
                            String uri = strs[4];
                            String cookie = strs[5];
                            for (String appVersion : fakerApp.getVersion()) {
                                if (StringUtils.isNotEmpty(appVersion)) {
                                    String[] version = appVersion.split(":", -1);
                                    if (version[0].equalsIgnoreCase("Uri")) {
                                        if (uri.contains(version[1])) {
                                            hasAppVersion = true;
                                            break;
                                        }
                                    } else if (version[0].equalsIgnoreCase("Cookie")) {
                                        if (cookie.contains(version[1])) {
                                            hasAppVersion = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }else{
                            hasAppVersion = true;
                            break;
                        }
                        }
                    }
                }
                float frequency = count / listsize;
                if (hasAppVersion && frequency >= hostThreshold)
                    return fakerApp.getName();
            }
        }
        return "";
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
        FileSystem tooFlileSystem = FileSystem.get(conf);
        InputStream browserVersionMapStream = tooFlileSystem.open(browserVersionMap);
        InputStream browserAppSystem = tooFlileSystem.open(browserApp);
        InputStream appFeatureSystem = tooFlileSystem.open(appFeature);
        InputStream browserFeatureSystem = tooFlileSystem.open(browserFeature);
        InputStream dataRelationSystem = tooFlileSystem.open(dataRelation);

        ReadXml readXml = new ReadXml(browserVersionMapStream,
                browserAppSystem,
                appFeatureSystem,
                browserFeatureSystem,
                dataRelationSystem);
        return  readXml;
    }

    //关系提取 数据关联
    private Set<String>  dataRelationExtract (ResultInfo info){
        Set<String> set = new HashSet<String>();
        //三层循环
        for (RelationVO relationVO : dataRelationList) {
            keyfor2: for (FieldVO keyFieldVO : relationVO.getKeyList()) {
                String keyStr = keyFieldVO.getField();
                String keyInfo = "";
                if (!Strings.isNullOrEmpty(keyStr) && keyStr.contains("_")){
                    for (String str : keyStr.split("_")) {
                        if (!Strings.isNullOrEmpty(getField(str,info))) {
                            if (StringUtils.isEmpty(keyInfo)) {
                                keyInfo += getField(str, info);
                            } else {
                                keyInfo += "_" + getField(str, info);
                            }
                        }else {
                            break keyfor2;
                        }
                    }
                }else{
                    keyInfo = getField(keyStr,info);
                }
                if (Strings.isNullOrEmpty(keyInfo)){
                    break keyfor2;
                }
               valuefor3 :for (FieldVO valueFieldVO : relationVO.getValueList()) {
                    sb.setLength(0);
                    String valueStr = valueFieldVO.getField();
                    String valueInfo = "";

                    if (!Strings.isNullOrEmpty(valueStr) && valueStr.contains("_")){
                        for (String str : valueStr.split("_")) {
                            if (!Strings.isNullOrEmpty(getField(str,info))) {
                                if (StringUtils.isEmpty(valueInfo)) {
                                    valueInfo += getField(str, info);
                                } else {
                                    valueInfo += "_" + getField(str, info);
                                }
                            }else {
                                break valuefor3;
                            }
                        }
                    }else{
                        valueInfo = getField(valueStr,info);
                    }
                    if (!Strings.isNullOrEmpty(keyInfo) && !Strings.isNullOrEmpty(valueInfo)) {
                        sb.append(keyInfo).append(PerConstants.SEPARATOR)
                                .append(keyFieldVO.getType()).append(PerConstants.SEPARATOR)
                                .append(valueInfo).append(PerConstants.SEPARATOR)
                                .append(valueFieldVO.getType()).append(PerConstants.SEPARATOR);
                        set.add(sb.toString());
                    }
                }
            }
        }
        return set;
    }
    // 提取数据，
    public String getField(String str,ResultInfo info){
        switch (str) {
            case "SRCIP":
                return info.getSrcip();
            case "MAC":
                return info.getMac() ;
            case "IMEI":
                return info.getImei();
            case "TERMINALCOMBINE":
                return info.getCombine();
            case "OS":
                return info.getOs();
            case "DEVICETYPE":
                return info.getDevicetype();
            case "BROWSER":
                return info.getBrowser();
            case "USERINFO":
                return info.getUserinfo();
            case "APPNAME":
                return info.getAppname();
            case "WEBSITE":
                return info.getWebsite();
            default:
                return "";
        }
    }

    public static void main(String[] args) {

        String[] longs = {"1526093208976","1526093087593","1526092999849","1526093051045","1526093063946","1526093052339","1526093068493"};
        List<String> longList = Arrays.asList(longs);
        List arrList = new ArrayList(longList);
        System.out.println(new PersonasReducer().calcuFrequrncy(arrList));
    }
}
