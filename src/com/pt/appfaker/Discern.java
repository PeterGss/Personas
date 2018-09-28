package com.pt.appfaker;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.xmlutils.Application;
import com.xmlutils.Browser;
import com.xmlutils.ReadXml;
import com.xmlutils.User;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;

public class Discern {
	static Pattern macRegex = Pattern.compile("([0-9a-fA-F]{2})(([/\\s:|-]?+[0-9a-fA-F]{2}){5})");
	static Pattern imeiRegex = Pattern.compile("^(\\d{15}|\\d{17})$");
	public static void main(String[] args) {
		Discern discern = new Discern();
		discern.setup();
		discern.map();
	}

	static private Map<String, Application> appFeatureMap;
	static private Map<String, Browser> browserFeatureMap;
	static private Map<String, String> osUnifyMap;
	
	public void setup() {
		ReadXml readXml = null;
		try {
			readXml = new ReadXml(new FileInputStream("analysis/conf/browserVersion.xml"),
					new FileInputStream("analysis/conf/browserApp.xml"), new FileInputStream("analysis/conf/appFeature.xml"),
					new FileInputStream("analysis/conf/browserFeature.xml"), new FileInputStream("analysis/conf/dataRelation.xml"),
					new FileInputStream("analysis/conf/osUnify.xml"),new FileInputStream("analysis/conf/osUnify.xml"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		appFeatureMap = readXml.getAppFeatureMap();
		browserFeatureMap = readXml.getBrowserFeatureMap();
		osUnifyMap = readXml.getOsUnifyMap();
	}

	public void map() {
		// 模拟数据 源数据
		String cookie = "o_cookie=01984582830; pac_uid=1_001984582830; pt2gguin=o01984582830; ptui_loginuin=001984582830; uin=o0001984582830; mac=28d244ffab90; hw_mac=28:d2:44:ff:ab:90";
		String uri = "weixin.qq.com?uin=190002135&imei=123456789963258&clientinfo=qq2014&devicetype=Windows+10";
		String host = "www.qq.com";
		// 模拟数据 提取结果数据
		String APPNAME = "微信";
		String BROWSER = "QQ Browser";
		String HOST = "";
		String WEBSITE = "";
		String MAC = "";
		String IMEI = "";
		String APPVERSION = "";
		String OS = "";
		String USERINFO = "";
		
		// 1 解析cookie和uri
		HashMap<String, String> cookieMap = parseCookie(cookie);
		HashMap<String, String> uriMap = parseURI(uri);
		// 2 如果是应用
		if (APPNAME != null) {
			Application application = appFeatureMap.get(APPNAME);
			if (application != null) {
				// 获取mac
				MAC = hashSetToStringFormat(getMac(cookieMap, uriMap, application.getMac()));
				// 获取imei
				IMEI = hashSetToStringFormat(getImei(cookieMap, uriMap, application.getImei()));
				// 获取应用版本
				APPVERSION = hashSetToStringFormat(getVersion(cookieMap, uriMap, application.getVersion()));
				// 获取os
				OS = hashSetToStringFormat(getOs(cookieMap, uriMap, application.getOs()));
				// 获取用户
				USERINFO = hashSetToStringFormat(getUser(cookieMap, uriMap, application.getUser()));
			}
		}
		System.out.println("MAC : " + MAC);
		System.out.println("IMEI : " + IMEI);
		System.out.println("APPVERSION : " + APPVERSION);
		System.out.println("OS : " + OS);
		System.out.println("USERINFO : " + USERINFO);

		// 3 如果是浏览器
		if (BROWSER != null) {
			// 通过一级域名，判断访问的是什么网址
			for (Entry<String, Browser> entry : browserFeatureMap.entrySet()) {
				if (host.contains(entry.getKey())) {
					HOST = entry.getKey();
					break;
				}
			}
			Browser browser = browserFeatureMap.get(HOST);
			if (browser != null) {
				// 访问网址
				WEBSITE = browser.getProduct();
				// 获取mac
				MAC = hashSetToStringFormat(getMac(cookieMap, uriMap, browser.getMac()));
				// 获取imei
				IMEI = hashSetToStringFormat(getImei(cookieMap, uriMap, browser.getImei()));
				// 获取用户
				USERINFO = hashSetToStringFormat(getUser(cookieMap, uriMap, browser.getUser()));
			}
			System.out.println("HOST : " + HOST);
			System.out.println("WEBSITE : " + WEBSITE);
			System.out.println("MAC : " + MAC);
			System.out.println("IMEI : " + IMEI);
			System.out.println("USERINFO : " + USERINFO);
		}
	
	}

	/**
	 * Cookie解析公用方法
	 * 
	 * @param cookie
	 * @return
	 */
	public static HashMap<String, String> parseCookie(String cookie) {
		HashMap<String, String> cookieMap = new HashMap<String, String>();
		if (cookie != null) {
			String[] cookieSubs = cookie.split(";");
			for (String cookieSub : cookieSubs) {
				int firstFlag = cookieSub.indexOf("=");
				if (firstFlag != -1) {
					String cookieKey = cookieSub.substring(0, firstFlag);
					cookieKey = cookieKey.trim();
					String cookieValue = cookieSub.substring(firstFlag + 1);
					if (cookieKey.equalsIgnoreCase("mac") || cookieKey.equalsIgnoreCase("imei")) {
						cookieKey = cookieKey.toLowerCase();
					}
					cookieMap.put(cookieKey, cookieValue);
				}
			}
		}
		return cookieMap;
	}

	/**
	 * URI解析公用方法
	 * 
	 * @param uri
	 * @return
	 */
	public static HashMap<String, String> parseURI(String uri) {
		HashMap<String, String> uriMap = new HashMap<String, String>();
		if (uri != null) {
			int firstQuestionMark = uri.indexOf("?");
			if (firstQuestionMark != -1) {
				String uriParamStr = uri.substring(firstQuestionMark + 1);
				if (!uriParamStr.trim().isEmpty()) {
					String[] uriParams = uriParamStr.split("&");
					for (String uriParam : uriParams) {
						int firstEqual = uriParam.indexOf("=");
						if (firstEqual != -1) {
							String uriKey = uriParam.substring(0, firstEqual);
							uriKey = uriKey.trim();
							String uriValue = uriParam.substring(firstEqual + 1);
							if (uriKey.equalsIgnoreCase("mac") || uriKey.equalsIgnoreCase("imei")) {
								uriKey = uriKey.toLowerCase();
							}
							uriMap.put(uriKey, uriValue);
						}
					}
				}
			}
		}
		return uriMap;
	}

	/**
	 * 获取mac
	 * 
	 * @param cookieMap
	 * @param URIMap
	 * @param macList
	 * @return
	 */
	public static HashSet<String> getMac(HashMap<String, String> cookieMap, HashMap<String, String> uriMap,
			List<String> macList) {
		String placeRegex = "(.{2})";
		HashSet<String> macSet = new HashSet<String>();
		ArrayList<String> macMiddleList = new ArrayList<String>();
		// 合并默认mac和配置文件中能提取mac的key
		ArrayList<String> cookieMacList = new ArrayList<String>();
		ArrayList<String> uriMacList = new ArrayList<String>();
		cookieMacList.add("mac");
		uriMacList.add("mac");
		for (String mac : macList) {
			String[] macSub = mac.split(":");
			if (macSub.length == 2) {
				switch (macSub[0]) {
				case "Cookie":
					if (!"mac".equalsIgnoreCase(macSub[1])) {
						cookieMacList.add(macSub[1]);
					}
					break;
				case "URI":
					if (!"mac".equalsIgnoreCase(macSub[1])) {
						uriMacList.add(macSub[1]);
					}
					break;
				default:
					break;
				}
			}
		}
		// 循环获取cookie中的mac
		for (String cookieMac : cookieMacList) {
			if (cookieMap.get(cookieMac) != null) {
				macMiddleList.add(cookieMap.get(cookieMac));
			}
		}
		// 循环获取uri中的mac
		for (String uriMac : uriMacList) {
			if (uriMap.get(uriMac) != null) {
				macMiddleList.add(uriMap.get(uriMac));
			}
		}
		// 验证mac地址有效性,并归一化mac值
		for (String macStr : macMiddleList) {
			if (macRegex.matcher(macStr).matches()) {
				macStr=macStr.toUpperCase();
				if(!macStr.contains("-")&&!macStr.contains(":")){
					macStr = macStr.replaceAll(placeRegex, "$1-");
					macStr = macStr.substring(0, macStr.length()-1);
				}if(macStr.contains(":")){
					macStr = macStr.replace(":", "-");
				}
				macSet.add(macStr);
			}
		}
		return macSet;
	}

	/**
	 * 获取imei
	 * 
	 * @param cookieMap
	 * @param URIMap
	 * @param imeiList
	 * @return
	 */
	public static HashSet<String> getImei(HashMap<String, String> cookieMap, HashMap<String, String> uriMap,
			List<String> imeiList) {
		HashSet<String> imeiSet = new HashSet<String>();
		ArrayList<String> imeiMiddleList = new ArrayList<String>();
		ArrayList<String> cookieImeiList = new ArrayList<String>();
		ArrayList<String> uriImeiList = new ArrayList<String>();
		cookieImeiList.add("imei");
		uriImeiList.add("imei");
		for (String imei : imeiList) {
			String[] macSub = imei.split(":");
			if (macSub.length == 2) {
				switch (macSub[0]) {
				case "Cookie":
					if (!"imei".equalsIgnoreCase(macSub[1])) {
						cookieImeiList.add(macSub[1]);
					}
					break;
				case "URI":
					if (!"imei".equalsIgnoreCase(macSub[1])) {
						uriImeiList.add(macSub[1]);
					}
					break;
				default:
					break;
				}
			}
		}
		// 循环获取cookie中的imei
		for (String cookieMac : cookieImeiList) {
			if (cookieMap.get(cookieMac) != null) {
				imeiMiddleList.add(cookieMap.get(cookieMac));
			}
		}
		// 循环获取uri中的imei
		for (String uriMac : uriImeiList) {
			if (uriMap.get(uriMac) != null) {
				imeiMiddleList.add(uriMap.get(uriMac));
			}
		}
		for (String imeiStr : imeiMiddleList) {
			if (imeiRegex.matcher(imeiStr).matches()) {
				imeiSet.add(imeiStr);
			}
		}
		return imeiSet;
	}

	/**
	 * 获取version
	 * 
	 * @param cookieMap
	 * @param uriMap
	 * @param list
	 * @return
	 */
	public static HashSet<String> getVersion(HashMap<String, String> cookieMap, HashMap<String, String> uriMap,
			List<String> versionList) {
		HashSet<String> appVersionSet = new HashSet<String>();
		for (String version : versionList) {
			String[] versions = version.split(":");
			if (versions.length == 2) {
				switch (versions[0]) {
				case "Cookie":
					if (cookieMap.get(versions[1]) != null) {
						appVersionSet.add(cookieMap.get(versions[1]));
					}
					break;
				case "URI":
					if (uriMap.get(versions[1]) != null) {
						appVersionSet.add(uriMap.get(versions[1]));
					}
					break;
				default:
					break;
				}
			}
		}
		return appVersionSet;
	}

	/**
	 * 获取os
	 * 
	 * @param cookieMap
	 * @param uriMap
	 * @param osList
	 * @return
	 */
	public static HashSet<String> getOs(HashMap<String, String> cookieMap, HashMap<String, String> uriMap,
			List<String> osList) {
		HashSet<String> osSet = new HashSet<String>();
		for (String os : osList) {
			String[] osSub = os.split(":");
			if (osSub.length == 2) {
				switch (osSub[0]) {
				case "Cookie":
					if (cookieMap.get(osSub[1]) != null) {
						osSet.add(osUnify(cookieMap.get(osSub[1])));
					}
					break;
				case "URI":
					if (uriMap.get(osSub[1]) != null) {
						osSet.add(osUnify(uriMap.get(osSub[1])));
					}
					break;
				default:
					break;
				}
			}
		}
		return osSet;
	}

	/**
	 * 操作系统归一化处理
	 * 
	 * @param os
	 * @return
	 */
	public static String osUnify(String os) {
		for (Entry<String, String> entry : osUnifyMap.entrySet()) {
			if (os.contains(entry.getKey())) {
				os = entry.getKey();
				break;
			}
		}
		if (osUnifyMap.get(os) == null) {
			return "";
		}
		return osUnifyMap.get(os);

	}

	/**
	 * 获取所有用户信息
	 * 
	 * @param cookieMap
	 * @param uriMap
	 * @param userList
	 * @return
	 */
	public static HashSet<String> getUser(HashMap<String, String> cookieMap, HashMap<String, String> uriMap,
			List<User> userList) {
		HashSet<String> userInfoSet = new HashSet<String>();
		for (User user : userList) {
			userInfoSet.addAll(
					getUser(cookieMap, uriMap, user.getType(), user.getValue(), user.getSplit(), user.getPosition()));
		}
		return userInfoSet;
	}

	/**
	 * 获取单个type的用户信息
	 * 
	 * @param cookieMap
	 * @param uriMap
	 * @param type
	 * @param userValue
	 * @param split
	 * @param position
	 * @return
	 */
	public static HashSet<String> getUser(HashMap<String, String> cookieMap, HashMap<String, String> uriMap, int type,
			List<String> userValueList, String split, int position) {
		HashSet<String> userInfoSet = new HashSet<String>();
		switch (type) {
		case 1:
			userInfoSet = type1(cookieMap, uriMap, userValueList);
			break;
		case 2:
			userInfoSet = type2(cookieMap, uriMap, userValueList, split, position);
			break;
		case 3:
			userInfoSet = type3(cookieMap, uriMap, userValueList);
			break;
		case 4:
			userInfoSet = type4(cookieMap, uriMap, userValueList, split);
			break;
		default:
			break;
		}
		return userInfoSet;
	}

	/**
	 * type1公用方法
	 * 
	 * @param cookieMap
	 * @param uriMap
	 * @param userValue
	 * @return
	 */
	public static HashSet<String> type1(HashMap<String, String> cookieMap, HashMap<String, String> uriMap,
			List<String> userValueList) {
		HashSet<String> userInfoSet = new HashSet<String>();
		for (String user : userValueList) {
			String[] userSub = user.split(":");
			// userSub的长度必须为2
			if (userSub.length == 2) {
				switch (userSub[0]) {
				case "Cookie":
					if (cookieMap.get(userSub[1]) != null) {
						String userInfo = cookieMap.get(userSub[1]);
						for(int i=0;i<userInfo.length();i++){
								if(0==Integer.parseInt(userInfo.substring(0, 1))){
									userInfo=userInfo.substring(1);
								}else {
									break;
							}
						}
						userInfoSet.add(userInfo);
					}
					break;
				case "URI":
					if (uriMap.get(userSub[1]) != null) {
						String userInfo =uriMap.get(userSub[1]);
						for(int i=0;i<userInfo.length();i++){
							if(0==Integer.parseInt(userInfo.substring(0, 1))){
								userInfo=userInfo.substring(1);
							}else {
								break;
							}
						}
						userInfoSet.add(userInfo);
					}
					break;
				default:
					break;
				}
			}
		}
		return userInfoSet;
	}

	/**
	 * type2公用方法
	 * 
	 * @param cookieMap
	 * @param uriMap
	 * @param userValue
	 * @param split
	 * @param position
	 * @return
	 */
	public static HashSet<String> type2(HashMap<String, String> cookieMap, HashMap<String, String> uriMap,
			List<String> userValueList, String split, int position) {
		HashSet<String> userInfoSet = new HashSet<String>();
		for (String user : userValueList) {
			String[] userSub = user.split(":");
			// userSub的长度必须为2
			if (userSub.length == 2) {
				switch (userSub[0]) {
				case "Cookie":
					if (cookieMap.get(userSub[1]) != null) {
						if (split != null && !split.isEmpty()) {
							String cookieValue = cookieMap.get(userSub[1]);
							String[] cookieValues = cookieValue.split(split);
							if (cookieValues.length > position) {
								String userInfo = cookieValues[position];
								for(int i=0;i<userInfo.length();i++){
									if(0==Integer.parseInt(userInfo.substring(0, 1))){
										userInfo=userInfo.substring(1);
									}else {
										break;
									}
								}
								userInfoSet.add(userInfo);
							}
						}
					}
					break;
				case "URI":
					if (uriMap.get(userSub[1]) != null) {
						if (split != null && !split.isEmpty()) {
							String uriValue = uriMap.get(userSub[1]);
							String[] uriValues = uriValue.split(split);
							if (uriValues.length > position) {
								String userInfo = uriValues[position];
								for(int i=0;i<userInfo.length();i++){
									if(0==Integer.parseInt(userInfo.substring(0, 1))){
										userInfo=userInfo.substring(1);
									}else {
										break;
									}
								}
								userInfoSet.add(userInfo);
							}
						}
					}
					break;
				default:
					break;
				}
			}
		}
		return userInfoSet;
	}

	/**
	 * type3公用方法
	 * 
	 * @param cookieMap
	 * @param uriMap
	 * @param userValue
	 * @return
	 */
	public static HashSet<String> type3(HashMap<String, String> cookieMap, HashMap<String, String> uriMap,
			List<String> userValueList) {
		HashSet<String> userInfoSet = new HashSet<String>();
		for (String user : userValueList) {
			String[] userSub = user.split(":");
			// userSub的长度必须为3
			if (userSub.length == 3) {
				switch (userSub[0]) {
				case "Cookie":
					if (cookieMap.get(userSub[1]) != null) {
						String cookieValueJson = cookieMap.get(userSub[1]);
						JsonParser jsonParser = new JsonParser();
						JsonObject jsonObject = (JsonObject) jsonParser.parse(cookieValueJson);
						if (jsonObject.get(userSub[2]) != null) {
							String userInfo = jsonObject.get(userSub[2]).toString().replaceAll("\"", "");
							for(int i=0;i<userInfo.length();i++){
								if(0==Integer.parseInt(userInfo.substring(0, 1))){
									userInfo=userInfo.substring(1);
								}else {
									break;
								}
							}
							userInfoSet.add(userInfo);
						}
					}
					break;
				case "URI":
					if (uriMap.get(userSub[1]) != null) {
						String uriValueJson = uriMap.get(userSub[1]);
						JsonParser jsonParser = new JsonParser();
						JsonObject jsonObject = (JsonObject) jsonParser.parse(uriValueJson);
						if (jsonObject.get(userSub[2]) != null) {
							String userInfo =jsonObject.get(userSub[2]).toString().replaceAll("\"", "");
							for(int i=0;i<userInfo.length();i++){
								if(0==Integer.parseInt(userInfo.substring(0, 1))){
									userInfo=userInfo.substring(1);
								}else {
									break;
								}
							}
							userInfoSet.add(userInfo);
						}
					}
					break;
				default:
					break;
				}
			}
		}
		return userInfoSet;
	}

	/**
	 * type4公用方法
	 * 
	 * @param cookieMap
	 * @param uriMap
	 * @param userValue
	 * @param split
	 * @return
	 */
	public static HashSet<String> type4(HashMap<String, String> cookieMap, HashMap<String, String> uriMap,
			List<String> userValueList, String split) {
		HashSet<String> userInfoSet = new HashSet<String>();
		for (String user : userValueList) {
			String[] userSub = user.split(":");
			// userSub的长度必须为3
			if (userSub.length == 3) {
				switch (userSub[0]) {
				case "Cookie":
					if (cookieMap.get(userSub[1]) != null) {
						String cookieValue = cookieMap.get(userSub[1]);
						String[] cookieValueSubs = cookieValue.split(split);
						for (String cookieValueSub : cookieValueSubs) {
							cookieValueSub = cookieValueSub.replaceAll("\"", "");
							int firstFlag = cookieValueSub.indexOf("=");
							if (firstFlag != -1) {
								String cookieValueSubKey = cookieValueSub.substring(0, firstFlag);
								cookieValueSubKey = cookieValueSubKey.trim();
								if (cookieValueSubKey.equals(userSub[2])) {
									String userInfo = cookieValueSub.substring(firstFlag + 1);
									for(int i=0;i<userInfo.length();i++){
										if(0==Integer.parseInt(userInfo.substring(0, 1))){
											userInfo=userInfo.substring(1);
										}else {
											break;
										}
									}
									userInfoSet.add(userInfo);
								}
							}
						}
					}
					break;
				case "URI":
					if (uriMap.get(userSub[1]) != null) {
						String uriValue = uriMap.get(userSub[1]);
						String[] uriValueSubs = uriValue.split(split);
						for (String uriValueSub : uriValueSubs) {
							uriValueSub = uriValueSub.replaceAll("\"", "");
							int firstFlag = uriValueSub.indexOf("=");
							if (firstFlag != -1) {
								String uriValueSubKey = uriValueSub.substring(0, firstFlag);
								uriValueSubKey = uriValueSubKey.trim();
								if (uriValueSubKey.equals(userSub[2])) {
									String userInfo =uriValueSub.substring(firstFlag + 1);
									for(int i=0;i<userInfo.length();i++){
										if(0==Integer.parseInt(userInfo.substring(0, 1))){
											userInfo=userInfo.substring(1);
										}else {
											break;
										}
									}
									userInfoSet.add(userInfo);
								}
							}
						}
					}
					break;
				default:
					break;
				}
			}
		}
		return userInfoSet;
	}

	/**
	 * 格式化hashSet的输出
	 *
	 * @param set
	 * @return
	 */
	public static String hashSetToStringFormat(HashSet<String> set) {
		if (set.size() == 0) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		for (String string : set) {
			sb.append(',').append(string);
		}
		sb.delete(0, 1);
		return sb.toString();
	}

}
