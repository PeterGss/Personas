package com.pt.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import com.useragentutils.UserAgent;
import com.useragentutils.UserAgentManager;

public class Demo {
	
	public static void main(String[] args) throws FileNotFoundException {
		String ua = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.26 Safari/537.36 Core/1.63.5702.400 QQBrowser/10.2.1893.400";
//		UserAgentManager userAgentManager = UserAgentManager.getInstance("conf/tool.xml", "conf/app.xml", "conf/browser.xml", "conf/os.xml");
		UserAgentManager userAgentManager = UserAgentManager.getInstance(new FileInputStream("conf/tool.xml"), new FileInputStream("conf/app.xml"), new FileInputStream("conf/browser.xml"), new FileInputStream("conf/os.xml"));
		UserAgent userAgent = userAgentManager.parseUserAgent(ua);
		System.out.println("getToolName()=" + userAgent.getToolName());
		System.out.println("getAppName()=" + userAgent.getAppName());
		System.out.println("getBrowserName()=" + userAgent.getBrowserName());
		System.out.println("getBrowserVersion()=" + userAgent.getBrowserVersion());
		System.out.println("getOSName()=" + userAgent.getOSName());
		System.out.println("getDeviceType()=" + userAgent.getDeviceType());
	}
	
}
