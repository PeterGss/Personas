package com.pt.util;

import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 公共方法类
 * @author Administrator
 *
 */
public class PublicFun {
    /**
     * 判断是否是windows操作系统
     * 
     * @return String
     */
    public static boolean isWindowsOS() {
        boolean isWindowsOS = false;
        String osName = System.getProperty("os.name");
        if (osName.toLowerCase().indexOf("windows") > -1) {
            isWindowsOS = true;
        }
        return isWindowsOS;
    }

    /**
     * 获取本机ip地址，并自动区分Windows还是linux操作系统
     * 
     * @return String
     */
    public static String getLocalHost() {
        String sIP = null;
        InetAddress ip = null;
        try {
            // 如果是Windows操作系统
            if (isWindowsOS()) {
                ip = InetAddress.getLocalHost();
            }// 如果是Linux操作系统
            else {
                boolean bFindIP = false;
                Enumeration<NetworkInterface> netInterfaces = NetworkInterface
                        .getNetworkInterfaces();
                while (netInterfaces.hasMoreElements()) {
                    if (bFindIP) {
                        break;
                    }

                    NetworkInterface ni = netInterfaces
                            .nextElement();
                    // ----------特定情况，可以考虑用ni.getName判断
                    // 遍历所有ip
                    Enumeration<InetAddress> ips = ni.getInetAddresses();
                    while (ips.hasMoreElements()) {
                        ip = ips.nextElement();
                        if (ip.isSiteLocalAddress() && !ip.isLoopbackAddress() // 127.开头的都是lookback地址
                                && ip.getHostAddress().indexOf(":") == -1) {
                            bFindIP = true;
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            MLogger.error(e.getMessage(), e);
        }
        if (null != ip) {
            sIP = ip.getHostAddress();
        }
        return sIP;
    }

    /**
     * 初始化log4j.properties
     * 
     * @return
     */
    public static void initLog4jConfig(String childDir, String logFileName) {
        String APP_HOME = System.getProperty("user.dir");
        if (isEmpty(logFileName)) {
            logFileName = "log4j.properties";
        }

        if (isEmpty(childDir)) {
            childDir = "conf";

        }
        if (isWindowsOS()) {
            PropertyConfigurator.configure(APP_HOME + File.separator + childDir + File.separator + logFileName);
        } else {
            PropertyConfigurator.configure(APP_HOME + File.separator + childDir + File.separator + logFileName);
        }
    }

    /**
     * 获取不同系统环境下的文件路径
     * 
     * @param childDir
     *            子目录
     * @param file
     *            文件名
     * @return
     */
    public static String getFilePath(String childDir, String file) {
        String APP_HOME = System.getProperty("user.dir");
        if (isWindowsOS()) {
            return APP_HOME + "\\" + childDir + "\\" + file;
        } else {
            return APP_HOME + "/../" + childDir + "/" + file;
        }
    }

    /**
     * 获取本机IP
     * 
     * @return ip
     */
/*    public static String getLocalHost() {
        InetAddress addr = null;
        try {
            addr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            logger.error(e.getMessage(), e);
        }

        return addr.getHostAddress().toString();
    }*/


    /**
     * 校验string是否为空
     * @param str 需要校验的字符串
     * @return boolean:长度大于0; false:为空或者长度等于0
     */
    public static boolean isEmpty(String str) {
        return (str == null || str.isEmpty());
    }
    
    /**
     * 校验string是否为null、空串或者空字符组成的串
     * @param str 需要校验的字符串
     * @return  false:为空或者trim后长度等于0
     */
    public static boolean isBlank(String str){
    	return (null == str || str.trim().isEmpty());
    }

    /**
     * 各种集合转换为逗号分隔的String，支持Vector, List, Set等等
     * 
     * @return 逗号分隔的字符串
     */
    public static String collect2Str(Object obj) {
        String str = null;
        str = obj.toString().replace(" ", "");
        str = str.substring(1, str.length() - 1);
        return str;
    }

    /**
     * 集合按要求拼接成string
     * 
     * @param
     */
    public static String collect2Str(Object obj, String linkSign) {
        String str = null;
        linkSign = linkSign.trim();
        if (obj == null) {
            return null;
        }
        str = obj.toString().replace(", ", linkSign);
        str = str.substring(1, str.length() - 1);
        return str;
    }

    /**
     * hashSet 转换为treeSet
     * 
     * @return treeSet<Integer>
     */
    public static TreeSet<Long> strSet2LongTreeSet(Set<String> strSet) {
        TreeSet<Long> intSet = new TreeSet<Long>();
        for (String str : strSet) {
            intSet.add(Long.valueOf(str));
        }
        return intSet;
    }

    

    /**
     * 字符串切割函数
     * @param srcStr 需要切割的字符串
     * @param regex 分隔符
     * @return list
     */
    public static List<String> split2List(String srcStr, String regex) {
        List<String> list = new ArrayList<String>();
        if (isEmpty(srcStr) || isEmpty(regex)) {
            return list;
        }
        String[] reslut = srcStr.split(regex);
        for (String str : reslut) {
            list.add(str);
        }
        return list;
    }
    
    /**
     * 字符串切割函数
     * @param srcStr 需要切割的字符串
     * @param regex 分隔符
     * @return set
     */
    public static Set<String> split2Set(String srcStr, String regex) {
        Set<String> set = new HashSet<String>();
        if (isEmpty(srcStr) || isEmpty(regex)) {
            return set;
        }
        String[] reslut = srcStr.split(regex);
        for (String string : reslut) {
            set.add(string);
        }
        return set;
    }

    /**
     * 字符串切割函数
     * @param srcStr 需要切割的字符串
     * @param regex 分隔符
     * @param set 存储
     * @return 
     */
    public static Set<String> split2Set(String srcStr, String regex, Set<String> set) {
        if (set == null || isEmpty(srcStr) || isEmpty(regex)) {
            return null;
        }
        String[] reslut = srcStr.split(regex);
        for (String string : reslut) {
            set.add(string);
        }
        
        return set;
    }

    /**
     * 将配置文件中的索引映射，切分成map
     * 格式 ： Key1:1,Key2:2,Key3:3
     *
     * @return
     */
    public static Map<String, Integer> split2IndexMap(String indexStr) {
        Map<String, Integer> indexMap = new HashMap<String, Integer>();
        if (null == indexStr || indexStr.isEmpty()) return indexMap;

        String[] valArr = indexStr.split(",");
        if (null == valArr || valArr.length == 0) return indexMap;

        for (String val : valArr) {
            String[] mapArr = val.split(":");
            if (null == mapArr || mapArr.length != 2) continue;
            indexMap.put(mapArr[0].toLowerCase(), Integer.valueOf(mapArr[1]));
        }
        return indexMap;
    }


    /**
     * 多组字符串切割
     *

     * @return set
     */
    public static Map<String,Set<String>> split2Map(String srcStr, String groupRegex, String keyValRegex, String valRegex) {
        Map<String,Set<String>> map = new HashMap<String, Set<String>>();
        Set<String> set = null;
        String[] reslut = null;
        if (isEmpty(srcStr) || isEmpty(groupRegex) || isEmpty(keyValRegex) || isEmpty(valRegex)) {
            return null;
        }
        try {
            String[] groups = srcStr.split(groupRegex, -1);
            String[] keyvals;
            for (String group : groups) {
                keyvals = group.split(keyValRegex, -1);
                if (keyvals.length != 2) {
                    return null;
                }
        
                reslut = keyvals[1].split(valRegex, -1);
                set = new HashSet<String>();
                for (String string : reslut) {
                    set.add(string);
                }
                
                if (set != null && set.size() > 0) {
                    map.put(keyvals[0], set);
                }
            }
        } catch (Exception e) {
            return null;
        }
        
        return map;
    }
    

    public static boolean isPositiveDigit(String str) {
        if (str.length() == 10 && !str.startsWith("1")){
            return false;
        }
        return (!isEmpty(str) && str.matches("[0-9]+")) || ("-1".equals(str));
    }

    /**
     * 获取函数名
     * 
     * @return 函数名
     */
    public static String getMethodName() {
        return Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    /**
     * 获取当前行号
     * 
     * @return 获取当前行号
     */
    public static int getLineNumber() {
        return Thread.currentThread().getStackTrace()[2].getLineNumber();
    }

    /**
     * 转换掉字符串中会引起xml失败的特殊字符
     * 
     * @param s
     */
    public static String xmlFormat(String str) {
        if (str == null) {
            return str;
        }
        str = str.replaceAll("<", "&lt;").replaceAll(">", "&gt;")
                .replaceAll("&", "&amp;").replaceAll("'", "&apos;")
                .replaceAll("\"", "&quot;")
                .replaceAll("[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f\\x7f]", "");
        // .replaceAll("[//x00-//x08//x0b-//x0c//0e-//1f//7f]", "")

        return str;
    }

    /**
     * 绝对秒数转换为年月日格式日期
     * @param intTime 绝对秒数
     */
    public static String sec2Data(SimpleDateFormat dateFormat, Object sec) throws Exception{
        String date = null;
        if (sec instanceof Long) {
            date = dateFormat.format(new Date((Long)sec * 1000L));
        }else if (sec instanceof String) {
            date = dateFormat.format(new Date(Integer.parseInt((String)sec) * 1000L));
        }else if (sec instanceof Integer) {
            date = dateFormat.format(new Date((Integer)sec * 1000L));
        }
     
        return date;
    }
    
    /**
     * 日期转化为绝对秒数
     * @param date
     * @return 秒数
     * @throws Exception
     */
    public static long date2Sec(SimpleDateFormat dateFormat, String date) throws Exception{
        return dateFormat.parse(date).getTime()/1000L;
    }

    /**
     * 判断目录下某个文件是否存在
     * @param fileName全路径文件名
     */
    public static boolean isFileExists(String fileName) {
        File file = new File(fileName);
        return file.exists() && file.isFile();

    }

    /**
     * 基站十进制转16进制
     * 
     * @param ecgi
     *            ：十进制基站号
     */
    public static String ecgiInt2Hex(String ecgi) {
        if (isEmpty(ecgi) || ecgi.indexOf("-") == -1) {
            return ecgi;
        }
        String[] strs = ecgi.split("-");
        if (strs.length != 4) {
            return ecgi;
        }

        String str3 = Integer.toHexString(Integer.valueOf(strs[3]));
        if (str3.length() < 2) {
            str3 = "0" + str3;
        }
        /*
         * System.out.println( strs[0]); System.out.println( strs[1]);
         * System.out.println( Integer.toHexString(Integer.valueOf(strs[2])));
         * System.out.println( str3);
         */
        return strs[0] + strs[1]
                + Integer.toHexString(Integer.valueOf(strs[2])) + str3;
    }

    /**
     * 基站16进制转十进制

     */
    public static String ecgiHex2Int(String ecgi) {
        if (isEmpty(ecgi) || ecgi.length() < 8) {
            return ecgi;
        }
        
        if (ecgi.indexOf("-") != -1) {
            return ecgi;
        }
        String str0 = ecgi.substring(0, 3);
        String str1 = ecgi.substring(3, 5);
        String str2 = ecgi.substring(5, ecgi.length() - 2);
        str2 = String.valueOf(Integer.parseInt(str2, 16));

        String str3 = ecgi.substring(ecgi.length() - 2);
        str3 = String.valueOf(Integer.parseInt(str3, 16));

        return str0 + "-" + str1 + "-" + str2 + "-" + str3;
    }

    public static boolean checkVerifyByPattern(String str, String pattern) {
        if (isEmpty(pattern) || isEmpty(str)) {
            return false;
        }

        Matcher m =  Pattern.compile(pattern).matcher(str);
        return m.matches();
    }

    /**
     * 8位的时间格式转换成绝对秒数
     * @param time
     * @return
     */
    public static long timeStr2int8(String time) {
        DateFormat df = new SimpleDateFormat("yyyyMMdd");
        Date d = null;
        long retDate = 0L;
        try {
            d = df.parse(time);
            retDate = d.getTime() / 1000L;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retDate;
    }

    /**
     * 检查字符串长度，如果大于等于指定长度返回true，否则返回false
     * @param str 检查字符串
     * @param length 长度
     */
    public static boolean strLengthVerify(String str, int length) {
        if (str == null) {
            return false;
        }
        return str.trim().length() >= length;
    }

    /**
     * ip转换成long型数字
     * @param ipAddress
     * @return
     */
    public static long ipToLong(String ipAddress) {

        if (ipAddress == null || ipAddress.equals("")) {
            return 0L;
        }
        long result = 0;
        try {
            String[] ipAddressInArray = ipAddress.split("\\.");

            for (int i = 0; i < ipAddressInArray.length; i++) {
                int power = 3 - i;
                int ip = Integer.parseInt(ipAddressInArray[i]);

                result += ip << 8 * power;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * 判断是否是IP
     * @param str
     * @return
     */
    public static boolean isDotIP(String str) {
        if (isEmpty(str)) {
            return false;
        }

        str = str.trim();
        String test = "^(\\d|[0-9]\\d|[01]\\d\\d|[2][0-4]\\d|[2][5][0-5])(\\.(\\d|[0-9]\\d|[01]\\d\\d|[2][0-4]\\d|[2][5][0-5])){3}$";

        Pattern pattern = Pattern.compile(test);
        Matcher matcher = pattern.matcher(str);

        return matcher.matches();
    }

    /**
     * Dot类型的IP转换成Int
     * 
     * @param ipDot
     * @return
     */
    public static int ipString2Int(String ipDot) {
        int ip = 0;

        if ((ipDot == null) || (ipDot.equals("")))
            return 0;
        try {
            int[] b = new int[4];
            String[] s = new String[4];
            StringTokenizer st = new StringTokenizer(ipDot, ".");

            for (int i = 0; i < 4; ++i) {
                s[i] = st.nextToken();
                if (Integer.parseInt(s[i].trim()) < 0)
                    b[i] = (Integer.parseInt(s[i].trim()) + 256);
                else
                    b[i] = Integer.parseInt(s[i].trim());
                ip += (b[i] << 8 * (3 - i));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ip;
    }

    public static long getTimeStamp(String timeStr) throws Exception {
        if (null == timeStr || timeStr.isEmpty()) return 0l;
        SimpleDateFormat dataFormat = null;
        if (timeStr.startsWith("2") && timeStr.length() == 12) {
            dataFormat = new SimpleDateFormat("yyyyMMddHHmm");
            return dataFormat.parse(timeStr).getTime() / 1000;
        } else if (timeStr.startsWith("2") && timeStr.length() == 8) {
            dataFormat = new SimpleDateFormat("yyyyMMdd");
            return dataFormat.parse(timeStr).getTime() / 1000;
        } else return Long.parseLong(timeStr);
    }

    public static void main(String[] args) {
        try {
            System.out.println(PublicFun.getTimeStamp("201803011203"));
        } catch (Exception e) {
        }
    }
}
