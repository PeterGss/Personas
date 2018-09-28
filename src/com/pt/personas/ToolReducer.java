package com.pt.personas;

import com.google.common.base.Strings;
import com.pt.util.MLogger;
import com.useragentutils.UserAgentManager;
import com.xmlutils.Application;
import com.xmlutils.Browser;
import com.xmlutils.ReadXml;
import com.xmlutils.datarelationvo.RelationVO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by Shaon on 2018/9/18.
 */
public class ToolReducer extends Reducer<Text, Text, Text, Text> {
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

        ResultInfo info = new ResultInfo();
        float frequency;
        for (Text value : values) {
            String valueStr = value.toString();
            String[] valueStrs = valueStr.split(PerConstants.SEPARATOR,-1);
            if (!Strings.isNullOrEmpty(valueStrs[1])){
                hostList.add(valueStrs[1]);
            }
            if (!Strings.isNullOrEmpty(valueStrs[6])){
                rectimeList.add(valueStrs[6]);
            }
            if (!Strings.isNullOrEmpty(valueStr)){
                valueList.add(valueStr);
            }

        }
        Collections.sort(rectimeList);
        String appFaker ="";
        switch (keys[0]) {
            // 工具类计算频率
            case PerConstants.FREQUENCY:
                if (!Strings.isNullOrEmpty(keys[1]) && !Strings.isNullOrEmpty(keys[2])) {
                    frequency = calcuFrequrncy(rectimeList);
                    for (String s : valueList) {
                        context.write(new Text(key.toString() + PerConstants.SEPARATOR + s), new Text(Float.toString(frequency)));
                    }
                }
                break;
            // 工具类计算频率
            case PerConstants.FREQUENCYTOOL:
                frequency = calcuFrequrncy(rectimeList);
                for (String s : valueList) {
                    context.write(new Text(key.toString() + PerConstants.SEPARATOR + s), new Text(Float.toString(frequency)));
                }
                break;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
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
