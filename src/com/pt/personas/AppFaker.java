package com.pt.personas;

/**
 * Created by Shaon on 2018/9/3.
 */
public class AppFaker {
    String appname;
    String appUseragent[];
    String appHosts[];
    String appReferer[];
    String appCookieCharacter[];
    String appUriCharacter[];

    public AppFaker(String appname, String[] appUseragent, String[] appHosts, String[] appReferer, String[] appCookieCharacter, String[] appUriCharacter) {
        this.appname = appname;
        this.appUseragent = appUseragent;
        this.appHosts = appHosts;
        this.appReferer = appReferer;
        this.appCookieCharacter = appCookieCharacter;
        this.appUriCharacter = appUriCharacter;
    }

    public String getAppname() {
        return appname;
    }

    public void setAppname(String appname) {
        this.appname = appname;
    }

    public String[] getAppUseragent() {
        return appUseragent;
    }

    public void setAppUseragent(String[] appUseragent) {
        this.appUseragent = appUseragent;
    }

    public String[] getAppHosts() {
        return appHosts;
    }

    public void setAppHosts(String[] appHosts) {
        this.appHosts = appHosts;
    }

    public String[] getAppReferer() {
        return appReferer;
    }

    public void setAppReferer(String[] appReferer) {
        this.appReferer = appReferer;
    }

    public String[] getAppCookieCharacter() {
        return appCookieCharacter;
    }

    public void setAppCookieCharacter(String[] appCookieCharacter) {
        this.appCookieCharacter = appCookieCharacter;
    }

    public String[] getAppUriCharacter() {
        return appUriCharacter;
    }

    public void setAppUriCharacter(String[] appUriCharacter) {
        this.appUriCharacter = appUriCharacter;
    }
}
