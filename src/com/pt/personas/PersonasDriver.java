package com.pt.personas;

import com.pt.util.MLogger;
import com.pt.util.TogetherFun;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Shaon on 2018/8/24.
 *
 */
public class PersonasDriver {
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
        if (pathList.size() == 0) {
            MLogger.warn(jobName + "  job no input path");
            return false;
        }
        Job job = Job.getInstance(conf, jobName  + today);
        job.setMapperClass(PersonasMapper.class);
        job.setJarByClass(PersonasDriver.class);
        job.setReducerClass(PersonasReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(getReducerNum(pathList, reduceHandleSize));
        for (Path path : pathList) {
            FileInputFormat.addInputPath(job, path);
        }
        job.setOutputFormatClass(TextOutputFormat.class);

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
        PersonasDriver driver = new PersonasDriver();
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