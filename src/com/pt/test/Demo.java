package com.pt.test;

import com.useragentutils.UserAgent;
import com.useragentutils.UserAgentManager;

public class Demo {
	
	public static void main(String[] args) {
		String ua = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.26 Safari/537.36";
		UserAgentManager userAgentManager = UserAgentManager.getInstance("conf/tool.xml", "conf/app.xml", "conf/browser.xml", "conf/os.xml");
		UserAgent userAgent = userAgentManager.parseUserAgent(ua);
		System.out.println("getToolName()=" + userAgent.getToolName());
		System.out.println("getAppName()=" + userAgent.getAppName());
		System.out.println("getBrowserName()=" + userAgent.getBrowserName());
		System.out.println("getBrowserVersion()=" + userAgent.getBrowserVersion());
		System.out.println("getOSName()=" + userAgent.getOSName());
		System.out.println("getDeviceType()=" + userAgent.getDeviceType());
	}
	
}
