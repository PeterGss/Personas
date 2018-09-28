package com.pt.appfaker;

/**
 * Created by Shaon on 2018/9/4.
 */
public class BrowerUserCharacter {
        String host;
        String browerField;
        String []field;
        String fieldSplit;

    public BrowerUserCharacter(String host, String uriOrCookie, String[] field, String fieldSplit) {
        this.host = host;
        this.browerField = uriOrCookie;
        this.field = field;
        this.fieldSplit = fieldSplit;
    }

    public String[] getField() {
        return field;
    }

    public void setField(String[] field) {
        this.field = field;
    }

    public String getBrowerField() {
        return browerField;
    }

    public void setBrowerField(String browerField) {
        this.browerField = browerField;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }



    public String getFieldSplit() {
        return fieldSplit;
    }

    public void setFieldSplit(String fieldSplit) {
        this.fieldSplit = fieldSplit;
    }
}
