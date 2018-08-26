package com.pt.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.io.File;

public class MLogger {
    static final String FQCN = MLogger.class.getName();
    public static Logger logger = Logger.getLogger(MLogger.class);

    static {//首先加一步校验，才能在执行静态代码块中初始化日志插件配置文件时报FileNotFoundException
        if(checkLogFileExits("conf", "log4j.properties"))
            initLog4jConfig("conf", "log4j.properties");
    }

    /**
     * 校验log4j.properties文件是否存在
     * @param childDir
     * @param logFile
     * @return
     */
    public static boolean checkLogFileExits(String childDir, String logFile)
    {
        String osName = System.getProperty("os.name");
        String filepath;
        if (osName.toLowerCase().indexOf("windows") > -1) {
            filepath=childDir + "\\" + logFile;
        } else {
            filepath="../" + childDir + "/" + logFile;
        }
        File logfile =new File(filepath);
        return logfile.exists();
    }
    
    public static void initLog4jConfig(String childDir, String logFile) {
        if (childDir == null || childDir.length() == 0) {
            childDir = "conf";
        }

        if (logFile == null || logFile.length() == 0) {
            logFile = "log4j.properties";
        }

        String osName = System.getProperty("os.name");
        try
        {
            if (osName.toLowerCase().indexOf("windows") > -1) {
                PropertyConfigurator.configure(childDir + "\\" + logFile);
            } else {
                PropertyConfigurator.configure("../" + childDir + "/" + logFile);
            }
        }
        catch(Exception e)
        {
            
        }
    }

    
    public static void debug(Object msg) {
        logger.log(FQCN, Level.DEBUG, msg, null);
    }

    public static void debug(String format, Object arg) {
        if (logger.isEnabledFor(Level.DEBUG)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            logger.log(FQCN, Level.DEBUG, ft.getMessage(), ft.getThrowable());
        }
    }

    public static void debug(String format, Object arg1, Object arg2) {
        if (logger.isEnabledFor(Level.DEBUG)) {
            FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
            logger.log(FQCN, Level.DEBUG, ft.getMessage(), ft.getThrowable());
        }
    }

    public static void debug(String format, Object... argArray) {
        if (logger.isEnabledFor(Level.DEBUG)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            logger.log(FQCN, Level.DEBUG, ft.getMessage(), ft.getThrowable());
        }
    }

    public static void info(Object msg) {
        logger.log(FQCN, Level.INFO, msg, null);
    }

    public static void info(String format, Object arg) {
        if (logger.isInfoEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            logger.log(FQCN, Level.INFO, ft.getMessage(), ft.getThrowable());
        }
    }

    public static void info(String format, Object arg1, Object arg2) {
        if (logger.isInfoEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
            logger.log(FQCN, Level.INFO, ft.getMessage(), ft.getThrowable());
        }
    }

    public static void info(String format, Object... argArray) {
        if (logger.isInfoEnabled()) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            logger.log(FQCN, Level.INFO, ft.getMessage(), ft.getThrowable());
        }
    }
    public static void warn(Object msg) {
        logger.log(FQCN, Level.WARN, msg, null);
    }

    public static void warn(String format, Object arg) {
        if (logger.isEnabledFor(Level.WARN)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            logger.log(FQCN, Level.WARN, ft.getMessage(), ft.getThrowable());
        }
    }

    public static void warn(String format, Object arg1, Object arg2) {
        if (logger.isEnabledFor(Level.WARN)) {
            FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
            logger.log(FQCN, Level.WARN, ft.getMessage(), ft.getThrowable());
        }
    }

    public static void warn(String format, Object... argArray) {
        if (logger.isEnabledFor(Level.WARN)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            logger.log(FQCN, Level.WARN, ft.getMessage(), ft.getThrowable());
        }
    }
    public static void error(Object msg) {
        logger.log(FQCN, Level.ERROR, msg, null);
    }

    public static void error(String format, Object arg) {
        if (logger.isEnabledFor(Level.ERROR)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            logger.log(FQCN, Level.ERROR, ft.getMessage(), ft.getThrowable());
        }
    }

    public static void error(String format, Object arg1, Object arg2) {
        if (logger.isEnabledFor(Level.ERROR)) {
            FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
            logger.log(FQCN, Level.ERROR, ft.getMessage(), ft.getThrowable());
        }
    }

    public static void error(String format, Object... argArray) {
        if (logger.isEnabledFor(Level.ERROR)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            logger.log(FQCN, Level.ERROR, ft.getMessage(), ft.getThrowable());
        }
    }    
}
