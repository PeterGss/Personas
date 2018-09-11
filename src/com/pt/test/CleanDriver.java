package com.pt.test;

import com.pt.personas.PersonasMapper;
import com.pt.personas.PersonasReducer;
import com.pt.util.MLogger;
import com.pt.util.TogetherFun;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Shaon on 2018/8/24.
 *
 */
public class CleanDriver {
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    protected Configuration conf = new Configuration();
    protected FileSystem fs;

    //项目名称
    protected String jobName;
    //hdfs系统上待分析文件目录
    protected String inputDir;
    //分析结果输出的根目录
    protected String outRootDir;
    //增量数据生成目录
    public String IncDetailOutDir = "";
    //全量数据生成目录
    public String hisDetailOutDir = "";
    //今天日期
    protected String today;
    //分析开始时间，从这个时间开始分析，一般手动执行时使用
    protected String startTime;
    //分析结束时间
    protected String endTime;
    //分析延迟时间
    protected int delayDays;
    //单个文件输出的最大条数限制
    protected int maxLine;
    //单个reduce处理的文件大小，单位:MB
    protected int reduceHandleSize;
    //上一次分析时间
    protected String lastAnalysisTime = "";

    /**
     * 加载配置
     */
    public void addResource() {
        conf.addResource("personas.xml");
        conf.addResource("tool.xml");
        conf.addResource("app.xml");
        conf.addResource("browser.xml");
        conf.addResource("os.xml");
        conf.addResource("application.xml");
        conf.addResource("explorer.xml");
    }

    /**
     * 初始化
     * @return
     * @throws Exception
     */
    protected boolean init() throws Exception {
        fs = FileSystem.get(conf);
        //加载分析业务对应的配置
        addResource();
        jobName = conf.get("jobname").toLowerCase().trim();
        inputDir = conf.get("inputdir").trim();
        if (inputDir.isEmpty()) {
            MLogger.error("no input path", jobName);
            return false;
        }
        outRootDir = conf.get("outrootdir").trim();
        if (outRootDir.isEmpty()) {
            MLogger.error("no output path ", jobName);
            return false;
        }
        if (outRootDir.lastIndexOf("/") != outRootDir.length() - 1) {
            outRootDir = outRootDir + "/";
        }
        outRootDir = outRootDir + jobName + "/";
        today = dateFormat.format(new Date());
        IncDetailOutDir = outRootDir + today + "/incdetail/" ;
        hisDetailOutDir = outRootDir + "history/;";

        startTime = conf.get("starttime", "0").trim();

        endTime = conf.get("endtime", dateFormat.format(new Date())).trim();

        delayDays = conf.getInt("delaydays", 0);//延时天数，如果获取不到则为0
        if (delayDays < 0) {
            delayDays = 0;
        }

        maxLine = conf.getInt("maxline", 10000);//输出文件最大的行数
        reduceHandleSize = conf.getInt("reduceHandleSize", 128);//reduce任务处理的数据大小

        //统计输出结果需要分析开始的时间
        return true;
    }
    /**
     * 启动函数、包括明细分析提取、统计分析
     * @throws Exception
     */
    public void run() throws Exception {
        if (!init()) {
            MLogger.error("init failed, return");
            return;
        }
        if (analysisJob()) {
            MLogger.info("job[{}] detail job success", jobName);
            //dataMove();
            //deleteTimeoutData(outRootDir);
        }
        else {
            MLogger.error("job[{}] detail job failed, exit", jobName);
        }
    }

    /**
     * 分析程序
     */
    public boolean analysisJob() throws Exception {
        List<Path> pathList = new ArrayList<Path>();
        lastAnalysisTime = TogetherFun.loadInputPath(jobName, inputDir,
                outRootDir, startTime, endTime, delayDays, pathList);
        MLogger.info("analysisJob() " + pathList.size());
        //pathList.add(new Path("F:\\BDB\\mr\\cpcenter\\Personas\\input\\CSMDP_REPORT_3_299.json"));
        if (pathList.size() == 0) {
            MLogger.warn(jobName + "  job no input path");
            return false;
        }
        Job job = Job.getInstance(conf, jobName  + today);
        job.setMapperClass(CleanMapper.class);
        job.setJarByClass(CleanDriver.class);
        job.setReducerClass(PersonasReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);
        //job.setNumReduceTasks(getReducerNum(pathList, reduceHandleSize));
        job.setNumReduceTasks(0);
        for (Path path : pathList) {
            FileInputFormat.addInputPath(job, path);
        }
        //job.setOutputFormatClass(TextOutputFormat.class);

        Path outputDir = new Path(IncDetailOutDir);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
            MLogger.info("delete success :" + outputDir.toString());
        }
        FileOutputFormat.setOutputPath(job, outputDir);
        job.waitForCompletion(true);
        if (job.isSuccessful()) {
            MLogger.info(jobName + "detail job analysis success!");
                       return true;
        } else {
            MLogger.error(jobName + "analyse job failed!");
        }
        return false;
    }


    /**
     * 计算需要多少reducer来处理文件
     *
     * @param list
     * @return
     * @throws IOException
     */
    public  int getReducerNum(List<Path> list, int reduceHandleSize) throws IOException {
        long length = 0;
        for (Path path : list) {
            length += getLength(path);
        }
        return getRound(length, reduceHandleSize);
    }

    private static int getRound(long length, int reduceHandleSize) {
        if (length == 0) {
            MLogger.warn("input path has no data, do not need to analysis, exit");
            return 0;
        }

        double tempnum = ((double) length / (double) (reduceHandleSize * 1024 * 1024L));
        int num = (int) Math.round(tempnum);
        // round方法，若文件过小则为0
        if (num == 0) {
            num = 1;
        }
        return num;
    }
    private  long getLength(Path path) throws IOException {
        long length = 0;
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatus = fs.listStatus(path);
        for (FileStatus status : fileStatus) {
            length += status.getLen();
        }
        return length;
    }

    /**
     * 程序入口函数
     *
     *
     */
    public static void main(String[] args) {
        CleanDriver driver = new CleanDriver();
        MLogger.info("PersonasDriver begin");
        try {
            driver.run();
            MLogger.info("PersonasDriver finish");
        } catch (Exception e) {
            MLogger.error("PersonasDriver job fail");
            MLogger.error(e.getMessage(), e);
        }
    }
}
