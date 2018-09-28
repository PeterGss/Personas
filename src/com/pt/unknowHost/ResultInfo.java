package com.pt.unknowHost;

/**
 * Created by Shaon on 2018/9/17.
 */
public class ResultInfo {
     private String srcip = "";
     private String mac = "";
     private String imei = "";
     private String combine = "";
     private String os = "";
     private String devicetype = "";
     private String browser = "";
     private String browserv = "";
     private String appname = "";
     private String appversion = "";
     private String tool = "";
     private String userinfo = "";
     private String website = "";

    public ResultInfo() {
    }


    public ResultInfo(ResultInfo info) {
        this.srcip = info.srcip;
        this.mac = info.mac;
        this.imei = info.imei;
        this.combine = info.combine;
        this.os = info.os;
        this.devicetype = info.devicetype;
        this.browser = info.browser;
        this.browserv = info.browserv;
        this.appname = info.appname;
        this.appversion = info.appversion;
        this.tool = info.tool;
        this.userinfo = info.userinfo;
        this.website = info.website;
    }

    public ResultInfo(String srcip, String mac, String imei, String os, String devicetype,  String appname, String appversion, String userinfo){
        this.srcip = srcip;
        this.mac = mac;
        this.imei = imei;
        this.os = os;
        this.devicetype = devicetype;
        this.appname = appname;
        this.appversion = appversion;
        this.userinfo = userinfo;
    }
    public String toAppResultInfo(){
        return srcip + PerConstants.SEPARATOR +
                mac + PerConstants.SEPARATOR +
                imei + PerConstants.SEPARATOR +
                os + PerConstants.SEPARATOR +
                devicetype + PerConstants.SEPARATOR +
                appname + PerConstants.SEPARATOR +
                appversion + PerConstants.SEPARATOR +
                userinfo;
    }

    public ResultInfo(String srcip, String mac, String imei, String combine, String os, String devicetype, String browser, String browserv, String appname, String appversion, String tool, String userinfo, String website) {
        this.srcip = srcip;
        this.mac = mac;
        this.imei = imei;
        this.combine = combine;
        this.os = os;
        this.devicetype = devicetype;
        this.browser = browser;
        this.browserv = browserv;
        this.appname = appname;
        this.appversion = appversion;
        this.tool = tool;
        this.userinfo = userinfo;
        this.website = website;
    }

    public String getSrcip() {
        return srcip;
    }

    public void setSrcip(String srcip) {
        this.srcip = srcip;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getCombine() {
        return combine;
    }

    public void setCombine(String combine) {
        this.combine = combine;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getDevicetype() {
        return devicetype;
    }

    public void setDevicetype(String devicetype) {
        this.devicetype = devicetype;
    }

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public String getBrowserv() {
        return browserv;
    }

    public void setBrowserv(String browserv) {
        this.browserv = browserv;
    }

    public String getAppname() {
        return appname;
    }

    public void setAppname(String appname) {
        this.appname = appname;
    }

    public String getAppversion() {
        return appversion;
    }

    public void setAppversion(String appversion) {
        this.appversion = appversion;
    }

    public String getTool() {
        return tool;
    }

    public void setTool(String tool) {
        this.tool = tool;
    }

    public String getUserinfo() {
        return userinfo;
    }

    public void setUserinfo(String userinfo) {
        this.userinfo = userinfo;
    }

    public String getWebsite() {
        return website;
    }

    public void setWebsite(String website) {
        this.website = website;
    }
}
