package com.pt.test;

import com.google.gson.Gson;
import com.pt.personas.Bean;
import eu.bitwalker.useragentutils.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by Shaon on 2018/8/24.
 */
public class Test {
    public static void main(String[] args) {
        Gson gson = new Gson();
        Bean bean = new Bean();
        /*String value ="{\"RecTime\":1526092689437,\"LogID\":36062315,\"LogType\":\"file_restore\",\"SrcIP\":\"172.16.33.64\",\"SrcPort\":41241,\"DstIP\":\"114.255.163.175\",\"DstPort\":80,\"sessionID\":\"19_1289418\",\"TransProto\":\"TCP\",\"AppProto\":\"HTTP\",\"TypeStr\":\"text/html\",\"Length\":389,\"Status\":\"REPEAT\",\"LocalUri\":\"/data/csmdp/data/var/http/19/235340\"}\n";
        bean = gson.fromJson(value,Bean.class);
        System.out.println(bean.toString());*/
        test0();



    }
    public static void test0(){
        UserAgent userAgent = UserAgent.parseUserAgentString("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.21 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.21");
        OperatingSystem os = userAgent.getOperatingSystem();
        String osName = os.getName();
        String deType = os.getDeviceType().getName();
        String osManufacturer = os.getManufacturer().getName();

        Browser bwr = userAgent.getBrowser();
        String bwrType = bwr.getBrowserType().getName();
        String bwrName = bwr.getName();
        String bwrEngine = bwr.getRenderingEngine().name();
        String bwrManufacturer = bwr.getManufacturer().getName();
        Version version = userAgent.getBrowserVersion();
        String bwrVersion = null;
        String bwrmajorVersion = null;
        String bwrminorVersion = null;
        if (version != null) {
            bwrVersion = version.getVersion();
            bwrmajorVersion = version.getMajorVersion();
            bwrminorVersion = version.getMinorVersion();
        }

        System.out.print("    osName : " + osName);
        System.out.print(", osManufacturer : " + osManufacturer);
        System.out.print(", deType : " + deType);
        System.out.print(", bwrName : " + bwrName);
        System.out.print(", bwrEngine : " + bwrEngine);
        System.out.print(", bwrManufacturer : " + bwrManufacturer);
        System.out.print(", bwrType : " + bwrType);
        System.out.print(", bwrVersion : " + bwrVersion);
        Application app = userAgent.getApplication();
        String appName = app.getName();
        String appType = app.getApplicationType().getName();
        String appManufacturer = app.getManufacturer().getName();
        System.out.print(", appName : " + appName);
        System.out.print(", appType : " + appType);
        System.out.print(", appManufacturer : " + appManufacturer);
        System.out.println();

    }
    public static void test1() {
        List<String> userAgentList = null;
        PrintWriter pw = null;
        try {
            userAgentList = FileUtils.readLines(new File("input/useragent.txt"));
            pw = new PrintWriter("output/outtest.txt");
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (String string : userAgentList) {
            string = getURLDecoderString(string);
            UserAgent userAgent = UserAgent.parseUserAgentString(string);

            OperatingSystem os = userAgent.getOperatingSystem();
            String osName = os.getName();
            String deType = os.getDeviceType().getName();
            String osManufacturer = os.getManufacturer().getName();

            Browser bwr = userAgent.getBrowser();
            String bwrType = bwr.getBrowserType().getName();
            String bwrName = bwr.getName();
            String bwrEngine = bwr.getRenderingEngine().name();
            String bwrManufacturer = bwr.getManufacturer().getName();

            Version version = userAgent.getBrowserVersion();
            String bwrVersion = null;
            @SuppressWarnings("unused")
            String bwrmajorVersion = null;
            @SuppressWarnings("unused")
            String bwrminorVersion = null;
            if (version != null) {
                bwrVersion = version.getVersion();
                bwrmajorVersion = version.getMajorVersion();
                bwrminorVersion = version.getMinorVersion();
            }

            Application app = userAgent.getApplication();
            String appName = app.getName();
            String appType = app.getApplicationType().getName();
            String appManufacturer = app.getManufacturer().getName();

            pw.println(string + " : ");
            pw.print("    osName : " + osName);
            pw.print(", osManufacturer : " + osManufacturer);
            pw.print(", deType : " + deType);
            pw.print(", bwrName : " + bwrName);
            pw.print(", bwrEngine : " + bwrEngine);
            pw.print(", bwrManufacturer : " + bwrManufacturer);
            pw.print(", bwrType : " + bwrType);
            pw.print(", bwrVersion : " + bwrVersion);
            pw.print(", appName : " + appName);
            pw.print(", appType : " + appType);
            pw.print(", appManufacturer : " + appManufacturer);
            pw.println();

//			pw.println("    userAgent: " + userAgent);

        }
        pw.flush();
        pw.close();
    }

    private static String getURLDecoderString(String str) {
        String result = "";
        if (null == str) {
            return "";
        }
            try {
                result = java.net.URLDecoder.decode(str, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
        }
        return result;
    }

    public static void test2() {
        UserAgent userAgent1 = UserAgent.parseUserAgentString("1");
        UserAgent userAgent2 = UserAgent.parseUserAgentString("mail.live.com");
        System.out.println(userAgent1 == userAgent2);
        System.out.println(userAgent1.equals(userAgent2));
    }
}
