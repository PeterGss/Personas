package com.pt.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TogetherFun {
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    private static FileSystem getFileSystem() throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        return fs;
    }

  
    /**
     * 校验胡户籍号是否有效，户籍号为6位数字
     * 
     * @param census
     * @return
     */
    public static boolean isValidCensus(String census) {
        return (census != null && census.length() == 6 && PublicFun.isPositiveDigit(census));
    }

    /**
     * 检查目录是否为空
     * 
     * @param path
     * @return
     * @throws IOException
     */
    public static boolean isDirEmpty(Path path) throws IOException {
        FileSystem fs = getFileSystem();
        // 检查该目录是否存在
        if (fs.exists(path)) {
            // 检查目录下是否有子目录或文件
            FileStatus[] fileStatus = fs.listStatus(path);
            return fileStatus.length == 0;
        } else {
            return true;
        }
    }

    /**
     * 
     * @param date
     *            需要处理的日期
     * @param days
     *            需要减去的天数
     * @return data -days
     * @throws Exception
     */
    public static String dateBefore(String date, int days) throws Exception {
        long time = dateFormat.parse(date).getTime() - days * 86400L * 1000L;
        date = dateFormat.format(new Date(time));
        return date;
    }

    /**
     * 获取当前日期，字符串格式：20150820
     * 
     * @return
     * @throws Exception
     */
    public static String getTodayDate() throws Exception {
        return dateFormat.format(new Date());
    }

    /**
     * 获取日期
     * 
     * @param time
     *            绝对秒数
     * @return 日期 如20150820
     * @throws Exception
     */
    public static String getDate(long time) throws Exception {
        return dateFormat.format(new Date(time));
    }

    /**
     * 计算需要多少reducer来处理文件
     * 
     * @param list
     * @return
     * @throws IOException
     */
    public static int getReducerNum(List<Path> list, int reduceHandleSize) throws IOException {
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

    private static long getLength(Path path) throws IOException {
        long length = 0;
        FileSystem fs = getFileSystem();
        FileStatus[] fileStatus = fs.listStatus(path);
        for (FileStatus status : fileStatus) {
            length += status.getLen();
        }
        return length;
    }

    /**
     * 获取需要处理的输入数据目录和上次分析时间
     * 
     * @param jobName
     *            分析job
     * @param inputDir
     *            原始输入路径
     * @param outRootDir
     *            输出路径
     * @param pathList
     *            需要处理的输入路径
     * @return 上次分析时间
     * @throws Exception
     */
    public static String loadInputPath(String jobName, String inputDir, String outRootDir,
            List<Path> pathList) throws Exception {
        return loadInputPath(jobName, inputDir, outRootDir, "0", dateFormat.format(new Date()), 0,
                pathList);
    }

    /**
     * 加载全路径、不过滤
     *
     * @param jobName
     *            正在工作的任务名称
     * @param inputDir
     *            输入目录
     * @return 路径集合
     * @throws Exception
     */
    public static List<Path> loadAllInputPath(String jobName, String inputDir) throws Exception {
        MLogger.info("job[{}] input path:" + inputDir, jobName);
        List<Path> pathList = new ArrayList<Path>();

        if (PublicFun.isEmpty(inputDir)) {
            MLogger.error("inputDir is null");
            return pathList;
        }

        FileSystem fs = getFileSystem();
        String[] inputPaths = inputDir.split(",");

        for (String inputPath : inputPaths) {
            // 获取需要分析的目录
            FileStatus[] inputFileStatusArray = fs.listStatus(new Path(inputPath));
            for (FileStatus fileStatus : inputFileStatusArray) {
                pathList.add(fileStatus.getPath());
            }
        }

        return pathList;
    }

    /**
     * 获取需要处理的输入数据目录和上次分析时间
     *
     * @param jobName
     *            分析job
     * @param inputDir
     *            原始输入路径
     * @param outRootDir
     *            输出路径
     * @param startTime
     *            分析开始时间
     * @param endTime
     *            分析结束时间
     * @param delayDays
     *            延迟天数
     * @param pathList
     *            需要处理的输入路径
     * @return 上次分析时间
     * @throws Exception
     */
    public static String loadInputPath(String jobName, String inputDir, String outRootDir,
                                       String startTime, String endTime, int delayDays, List<Path> pathList) throws Exception {
        MLogger.info("job[{}] input path: " + inputDir, jobName);
        MLogger.info("inputDir is " + inputDir + ";outRootDir is " + outRootDir + ";startTime is "
                + startTime + ";endTime is " + endTime);
        //TODO 测试
        if (pathList == null || PublicFun.isEmpty(inputDir) || PublicFun.isEmpty(outRootDir)
                || PublicFun.isEmpty(startTime) || PublicFun.isEmpty(endTime)) {
            MLogger.error("input param is null");
            return "";
        }
        MLogger.info("job[{}] outputpath:" + outRootDir, jobName);

        String lastAnalysisTime = "";
        FileSystem fs = getFileSystem();
        // 如果没有设置开始时间, 找出最后一次分析的时间，输出目录必须存在
        if ("0".equals(startTime) && fs.exists(new Path(outRootDir))) {
            FileStatus[] resFileStatusArray = fs.listStatus(new Path(outRootDir), new DataFilter());
            String pathName;
            for (FileStatus resFileStatus : resFileStatusArray) {
                pathName = resFileStatus.getPath().getName();
                if (lastAnalysisTime.compareTo(pathName) < 0) {
                    lastAnalysisTime = pathName;
                }
            }
            MLogger.info("lastAnalysisTime----: " + lastAnalysisTime);
            // 延时天数
            if (delayDays > 0 && lastAnalysisTime != null && lastAnalysisTime.length() == 8) {
                startTime = TogetherFun.dateBefore(lastAnalysisTime, delayDays);// 扩大startTime，以便包含延迟数据
            } else {
                startTime = lastAnalysisTime;
            }
        }

        MLogger.info("job[{}] lastAnalysisTime:{}", jobName, lastAnalysisTime);
        MLogger.info("job[{}] starttime:{}, endtime:{}", jobName, startTime, endTime);

        // 分析数据过滤器，包含增量数据和延时数据
        InputDataFilter filter = new InputDataFilter(startTime, endTime);
        // 增量数据过滤器
        InputDataFilter incFilter = new InputDataFilter(lastAnalysisTime, endTime);
        // 增量数据计数器
        int incCount = 0;
        String[] inputPaths = inputDir.split(",");
        for (String inputPath : inputPaths) {
            try {
                // 获取需要分析的目录
                FileStatus[] inputFileStatusArray = fs.listStatus(new Path(inputPath), filter);
                if (inputFileStatusArray.length == 0 || inputFileStatusArray == null) {
                    MLogger.info("job[{}] input path don't exist", jobName);
                }
                for (FileStatus fileStatus : inputFileStatusArray) {
                    pathList.add(fileStatus.getPath());
                }

                MLogger.info("job[{}]", jobName);
                FileStatus[] incInputFileStatusArray = fs.listStatus(new Path(inputPath),
                        incFilter);
                incCount += incInputFileStatusArray.length;
            } catch (Exception e) {
                MLogger.error(e+":message:"+e.getMessage());
                MLogger.error("job[{}] has no input path !", jobName);
            }
        }

        // 可能配置了延时时间，无增量数据时不做处理
        if (incCount == 0) {
            MLogger.warn("job[{}] no increment data", jobName);
            pathList.clear();
        }
        return lastAnalysisTime;
    }

    /**
     * 
     * loadInputPath 获取输入数据目录 key：配置输入目录最后一个单词，value：满足的输入目录集合
     * 如果输入目录是/MHLKJCGXX, 则key：mhlkjcgxx，
     * value：{/MHLKJCGXX/时间1,/MHLKJCGXX/时间2...}
     */
    public static Map<String, List<Path>> loadInputPath(String jobName, String inputDir,
                                                        String outRootDir, String startTime, String endTime, String lastAnalysisTime,
                                                        int delayDays, String today) throws Exception {
        MLogger.info("inputDir is " + inputDir + ";outRootDir is " + outRootDir + ";startTime is "
                + startTime + ";endTime is " + endTime);

        Map<String, List<Path>> inputMap = new HashMap<String, List<Path>>();
        FileSystem fs = getFileSystem();
        // 如果没有设置开始时间, 找出最后一次分析的时间
        if (startTime.trim().equals("0") && fs.exists(new Path(outRootDir))) {
            FileStatus[] resFileStatusArray = fs.listStatus(new Path(outRootDir), new DataFilter());
            for (FileStatus resFileStatus : resFileStatusArray) {
                String fileName = resFileStatus.getPath().getName();
                if (lastAnalysisTime.compareTo(fileName) < 0) {
                    lastAnalysisTime = fileName;
                }
            }
            // 延时天数
            if (delayDays > 0 && lastAnalysisTime != null && lastAnalysisTime.length() == 8) {
                startTime = TogetherFun.dateBefore(lastAnalysisTime, delayDays);
            } else {
                startTime = lastAnalysisTime;
            }
        }
        // endTime为空，则分析结束时间取当前时间
        if (PublicFun.isEmpty(endTime)) {
            endTime = today;
        }
        MLogger.info(jobName + " lastAnalysisTime: " + lastAnalysisTime);
        MLogger.info(jobName + " starttime: " + startTime + ", endtime: " + endTime);
        // 增量数据过滤器
        InputDataFilter incFilter = new InputDataFilter(lastAnalysisTime, endTime);
        // 增量数据计数器
        int incCount = 0;
        // 分析数据过滤器，包含增量数据和延时数据(如果没有延时就是增量数据，有的话就是延时数据)
        InputDataFilter filter = new InputDataFilter(startTime, endTime);
        String[] inputPaths = inputDir.split(",");
        for (String inputPath : inputPaths) {
            int index = inputPath.lastIndexOf("/");
            // 如果最后一个字符是反斜杠
            if (index == inputPath.length() - 1) {
                // 去掉最后一个字符
                inputPath = inputPath.substring(0, index);
                index = inputPath.lastIndexOf("/");
            }
            // 最后一个反斜杠之后单词作为key
            String tableName = inputPath.substring(index + 1).toLowerCase();
            FileStatus[] inputFileStatusArray = fs.listStatus(new Path(inputPath), filter);
            List<Path> inputList = new ArrayList<Path>();
            StringBuilder stringBuilder = new StringBuilder();
            MLogger.info("inputFileStatusArray length[{}]", inputFileStatusArray.length);
            for (FileStatus fileStatus : inputFileStatusArray) {
                inputList.add(fileStatus.getPath());
                stringBuilder.append(fileStatus.getPath().toUri().getPath()).append(",");
            }
            inputMap.put(tableName, inputList);
            MLogger.info("loadInputPath inputMap.size() " + inputMap.size());
            // 打印日志
            if (stringBuilder.length() >= 1) {
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            }
            // MLogger.info(jobName + " " + tableName + ": " + stringBuilder);
            FileStatus[] incInputFileStatusArray = fs.listStatus(new Path(inputPath), incFilter);
            incCount += incInputFileStatusArray.length;
        }
        if (incCount == 0) {
            MLogger.warn(jobName + ": no increment data");
            inputMap.clear();
        }
        return inputMap;

    }

    public static boolean checkDate(String date, SimpleDateFormat dateFormat) {
        try {
            dateFormat.parse(date);
        } catch (ParseException e) {
            return false;
        }
        return true;
    }

    /**
     * checkCapture(这里用一句话描述这个方法的作用)
     * 检查capturetime长度大于等于8，且为数字大于0，则返回true，否则返回false
     */
    public static boolean checkCaptureTime(String capturetime) {
        if (capturetime == null || capturetime.length() < 8) {
            return false;
        }
        try {
            return Long.parseLong(capturetime) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * 检查结束时间
     * 
     * @param capturetime
     *            20160101
     * @return
     */
    public static boolean checkEndTime(String capturetime) {
        return PublicFun.isEmpty(capturetime) || capturetime.length() == 8;
    }

    /**
     * 校验10位绝对秒数类型的时间格式
     * 
     * @param capturetime
     * @return
     */
    public static boolean checkCapture(String capturetime) {
        if (capturetime == null || capturetime.length() != 10) {
            return false;
        }
        try {
            return Long.parseLong(capturetime) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * 检查时间字段是否为10位绝对秒数
     * 
     * @param timeStr
     * @return
     * @author: mmliu
     * @DATE: 2016年1月5日下午1:27:51
     */
    public static boolean checkIntTimeValue(String timeStr) {
        return checkTimeValue(timeStr, 10);
    }

    /**
     * 检查时间字段是否是指定位数的绝对秒数
     * 
     * @param timeStr
     * @param len
     * @return
     * @author: mmliu
     * @DATE: 2016年1月5日下午1:28:22
     */
    public static boolean checkTimeValue(String timeStr, int len) {
        if (timeStr == null) {
            MLogger.error("param is null");
            return false;
        }

        if (timeStr.length() != len && timeStr.length() != len -1 && timeStr.length() != len-2) {
            MLogger.error("param [{}] is invalid, length[{}] is not {}", timeStr, timeStr.length(),
                    len);
            return false;
        }

        if (!PublicFun.isPositiveDigit(timeStr)) {
            MLogger.error("time[{}] is not all PositiveDigit", timeStr);
            return false;
        }

        return true;
    }

    /**
     * 指定日期格式时间转换为10位int秒数
     * 
     * @param timeStr
     *            时间字段
     * @param pattern
     *            时间格式 eg:yyyyMMdd
     * @return 10位绝对秒数
     * @throws Exception
     * @author: mmliu
     * @DATE: 2016年1月5日下午1:40:24
     */
    public static String get10IntTime(String timeStr, String pattern) throws Exception {
        if (pattern == null) {
            pattern = "yyyyMMdd";
        }

        if (!checkTimeValue(timeStr, pattern.length())) {
            return null;
        }

        return String.valueOf(PublicFun.date2Sec(new SimpleDateFormat(pattern), timeStr));
    }

    /**
     * 计算之前分析过的目录列表
     * @param //jobName
     * @param //inputDir
     * @param //startTime
     * @return
     * @throws IOException 
     */
    public static List<Path> loadOldAnalysisPath(String inputDir, String lastAnalyseTime) throws IOException {
        List<Path> pathList = new ArrayList<Path>();
        // 历史数据目录过滤器
        InputDataFilter incFilter = new InputDataFilter("0", lastAnalyseTime);
        FileSystem fs = getFileSystem();
        String[] inputPaths = inputDir.split(",");
        for (String inputPath : inputPaths) {
            try {
                // 获取需要分析的目录
                FileStatus[] inputFileStatusArray = fs.listStatus(new Path(inputPath), incFilter);
                if (inputFileStatusArray.length == 0 || inputFileStatusArray == null) {
                }
                for (FileStatus fileStatus : inputFileStatusArray) {
                    pathList.add(fileStatus.getPath());
                }
            } catch (Exception e) {
            }
        }

        return pathList;
    }

}
