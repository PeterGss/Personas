package com.pt.test;

import com.xmlutils.ReadXml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class Test {

	public static void main(String[] args) throws FileNotFoundException {
		System.out.println("1\t2\t3\t4".replace("\t",""));
	}
	
	public static void test1() throws FileNotFoundException {
		ReadXml readXml = new ReadXml(new FileInputStream("conf/browserVersion.xml"),
				new FileInputStream("conf/browserApp.xml"),
				new FileInputStream("conf/appFeature.xml"),
				new FileInputStream("conf/browserFeature.xml"),
				new FileInputStream("conf/dataRelation.xml"),
				new FileInputStream("conf/osUnify.xml")
				);
		System.out.println(readXml.getBrowserVersionMap().size());
		System.out.println(readXml.getBrowserVersionMap());
		System.out.println(readXml.getBrowserAppMap());
		System.out.println(readXml.getAppFeatureMap());
		System.out.println(readXml.getBrowserFeatureMap());
		System.out.println(readXml.getDataRelationList());
		System.out.println(readXml.getOsUnifyMap());
	}
	
	public static void test2() throws FileNotFoundException {
		ReadXml readXml = new ReadXml();
		readXml.setBrowserVersionXmlIn(new FileInputStream("conf/browserVersion.xml"));
		readXml.setBrowserAppXmlIn(new FileInputStream("conf/browserApp.xml"));
		readXml.setAppFeatureXmlIn(new FileInputStream("conf/appFeature.xml"));
		readXml.setBrowserFeatureXmlIn(new FileInputStream("conf/browserFeature.xml"));
		readXml.setDataRelationXmlIn(new FileInputStream("conf/dataRelation.xml"));
		readXml.setOsUnifyXmlIn(new FileInputStream("conf/osUnify.xml"));
		readXml.initReadXML();
		System.out.println(readXml.getBrowserVersionMap().size());
		System.out.println(readXml.getBrowserVersionMap());
		System.out.println(readXml.getBrowserAppMap());
		System.out.println(readXml.getAppFeatureMap());
		System.out.println(readXml.getBrowserFeatureMap());
		System.out.println(readXml.getDataRelationList());
		System.out.println(readXml.getOsUnifyMap());
	}
}
