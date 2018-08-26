package com.pt.personas;

/**
 * Created by Shaon on 2018/8/24.
 * 转json 数据
 */
public class Bean {
    String AppProto = "";
    String SrcIP = "";
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

    public Bean() {
    }

    public Bean(String appProto, String srcIP, String host, String localUri, String recTime, String logType, String srcPort, String uri, String cookie, String sessionID, String userAgent, String method, String setCookie, String resCode) {
        AppProto = appProto;
        SrcIP = srcIP;
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
                Host + "\t" +
                LocalUri + "\t" +
                RecTime + "\t" +
                LogType + "\t" +
                SrcPort + "\t" +
                Uri + "\t" +
                Cookie + "\t" +
                sessionID + "\t" +
                UserAgent + "\t" +
                Method + "\t" +
                SetCookie + "\t" +
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
