package com.pt.personas;

/**
 * Created by Shaon on 2018/8/24.
 * 转json 数据
 */
public class Bean {
    String AppProto = "";
    String SrcIP = "";
    String DstIP = "";
    String DstPort = "";
    String Host = "";
    String LocalUri = "";
    String RecTime = "";
    String LogType = "";
    String SrcPort = "";
    String Uri = "";
    String Cookie = "";
    String sessionID = "";
    String UserAgent = "";
    String Method = "";
    String SetCookie = "";
    String ResCode = "";
    String TTL = "";
    String method = "";

    public Bean() {
    }

    public Bean(String appProto, String srcIP, String dstIP, String dstPort, String host, String localUri, String recTime, String logType, String srcPort, String uri, String cookie, String sessionID, String userAgent, String method, String setCookie, String resCode, String TTL, String method1) {
        AppProto = appProto;
        SrcIP = srcIP;
        DstIP = dstIP;
        DstPort = dstPort;
        Host = host;
        LocalUri = localUri;
        RecTime = recTime;
        LogType = logType;
        SrcPort = srcPort;
        Uri = uri;
        Cookie = cookie;
        this.sessionID = sessionID;
        UserAgent = userAgent;
        Method = method;
        SetCookie = setCookie;
        ResCode = resCode;
        this.TTL = TTL;
        this.method = method1;
    }

    public String getDstIP() {
        return DstIP;
    }

    public void setDstIP(String dstIP) {
        DstIP = dstIP;
    }

    public String getDstPort() {
        return DstPort;
    }

    public void setDstPort(String dstPort) {
        DstPort = dstPort;
    }

    public String getTTL() {
        return TTL;
    }

    public void setTTL(String TTL) {
        this.TTL = TTL;
    }

    public String getAppProto() {
        return AppProto;
    }

    public void setAppProto(String appProto) {
        AppProto = appProto;
    }

    public String getSrcIP() {
        return SrcIP;
    }

    public void setSrcIP(String srcIP) {
        SrcIP = srcIP;
    }

    public String getHost() {
        return Host;
    }

    public void setHost(String host) {
        Host = host;
    }

    public String getLocalUri() {
        return LocalUri;
    }

    public void setLocalUri(String localUri) {
        LocalUri = localUri;
    }

    public String getRecTime() {
        return RecTime;
    }

    public void setRecTime(String recTime) {
        RecTime = recTime;
    }

    public String getLogType() {
        return LogType;
    }

    public void setLogType(String logType) {
        LogType = logType;
    }

    public String getSrcPort() {
        return SrcPort;
    }

    public void setSrcPort(String srcPort) {
        SrcPort = srcPort;
    }

    public String getUri() {
        return Uri;
    }

    public void setUri(String uri) {
        Uri = uri;
    }

    public String getCookie() {
        return Cookie;
    }

    public void setCookie(String cookie) {
        Cookie = cookie;
    }

    public String getSessionID() {
        return sessionID;
    }

    public void setSessionID(String sessionID) {
        this.sessionID = sessionID;
    }

    public String getUserAgent() {
        return UserAgent;
    }

    public void setUserAgent(String userAgent) {
        UserAgent = userAgent;
    }

    public String getMethod() {
        return Method;
    }

    public void setMethod(String method) {
        Method = method;
    }

    public String getSetCookie() {
        return SetCookie;
    }

    public void setSetCookie(String setCookie) {
        SetCookie = setCookie;
    }

    public String getResCode() {
        return ResCode;
    }

    public void setResCode(String resCode) {
        ResCode = resCode;
    }

    public String sepString(){
        return  AppProto + "\t" +
                SrcIP + "\t" +
                SrcPort + "\t" +
                DstIP + "\t" +
                DstPort + "\t" +
                Host + "\t" +
                LocalUri + "\t" +
                RecTime + "\t" +
                LogType + "\t" +
                Uri + "\t" +
                Cookie + "\t" +
                sessionID + "\t" +
                UserAgent + "\t" +
                Method + "\t" +
                SetCookie + "\t" +
                TTL + "\t" +
                method + "\t" +
                ResCode;
    }

    @Override
    public String toString() {
        return "Bean{" +
                "AppProto='" + AppProto + '\'' +
                ", SrcIP='" + SrcIP + '\'' +
                ", Host='" + Host + '\'' +
                ", LocalUri='" + LocalUri + '\'' +
                ", RecTime='" + RecTime + '\'' +
                ", LogType='" + LogType + '\'' +
                ", SrcPort='" + SrcPort + '\'' +
                ", Uri='" + Uri + '\'' +
                ", Cookie='" + Cookie + '\'' +
                ", sessionID='" + sessionID + '\'' +
                ", UserAgent='" + UserAgent + '\'' +
                ", Method='" + Method + '\'' +
                ", SetCookie='" + SetCookie + '\'' +
                ", ResCode='" + ResCode + '\'' +
                '}';
    }
}
