package com.pt.test;

import com.google.gson.JsonParser;
import com.xmlutils.ReadXml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.Date;

public class Test {
	private final static String ENCODE = "UTF-8";
	/**
	 * URL 解码
	 *
	 * @return String
	 * @author lifq
	 * @date 2015-3-17 下午04:09:51
	 */
	public static String getURLDecoderString(String str) {
		String result = "";
		if (null == str) {
			return "";
		}
		try {
			result = java.net.URLDecoder.decode(str, ENCODE);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return result;
	}
	/**
	 * URL 转码
	 *
	 * @return String
	 * @author lifq
	 * @date 2015-3-17 下午04:10:28
	 */
	public static String getURLEncoderString(String str) {
		String result = "";
		if (null == str) {
			return "";
		}
		try {
			result = java.net.URLEncoder.encode(str, ENCODE);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 *
	 * @return void
	 * @author lifq
	 * @date 2015-3-17 下午04:09:16
	 */
	public static void main(String[] args) {
		String str = "/cn/CreditCard/Special0ffers/All/qgsetcxykgcxcxdbjf.htm?merId=47886%27%29%20AND%20%28SELECT%203331%20FROM%28SELECT%20COUNT%28%2A%29%2CCONCAT%280x71717a7a71%2C%28SELECT%20%28ELT%283331%3D3331%2C1%29%29%29%2C0x71786a7a71%2CFLOOR%28RAND%280%29%2A2%29%29x%20FROM%20INFORMATION_SCHEMA.CHARACTER_SETS%20GROUP%20BY%20x%29a%29%20AND%20%28%27MIAq%27%3D%27MIAq&typeName=%E6%B1%BD%E8%BD%A6%E6%9C%8D%E5%8A%A1";
		System.out.println(getURLEncoderString(str));
		System.out.println(getURLDecoderString(str));

	}



}
