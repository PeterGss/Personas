package com.pt.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.net.URLDecoder;
import java.net.URLEncoder;

public class Utils {

    public static void storeObject(Object obj, Configuration conf, String key) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bout);
        bout = new ByteArrayOutputStream();
        out = new ObjectOutputStream(bout);
        out.writeObject(obj);
        out.flush();
        out.close();
        String s = bout.toString("ISO-8859-1");
        s = URLEncoder.encode(s, "UTF-8");
        conf.set(key, s);
    }

    public static Object loadObject(Configuration conf, String key) throws IOException {
        Object obj;
        // 反序列化
        String s = conf.get(key);
        s = URLDecoder.decode(s, "UTF-8");
        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(s.getBytes("ISO-8859-1")));
        try {
            obj = in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
        in.close();
        return obj;
    }

    public static void write(String str, DataOutput output) throws IOException {
        if (StringUtils.isEmpty(str)) {
            output.writeBoolean(false);
        } else {
            output.writeBoolean(true);
            output.writeUTF(str);
        }
    }

    public static String read(DataInput input) throws IOException {
        boolean notEmpty = input.readBoolean();
        if (notEmpty) {
            return input.readUTF();
        }
        return null;
    }
}
