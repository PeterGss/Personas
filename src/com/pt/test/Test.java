package com.pt.test;

import com.google.gson.Gson;
import com.pt.personas.Bean;

/**
 * Created by Shaon on 2018/8/24.
 */
public class Test {
    public static void main(String[] args) {
        Gson gson = new Gson();
        Bean bean = new Bean();
        String value ="{\"RecTime\":1526092689437,\"LogID\":36062315,\"LogType\":\"file_restore\",\"SrcIP\":\"172.16.33.64\",\"SrcPort\":41241,\"DstIP\":\"114.255.163.175\",\"DstPort\":80,\"sessionID\":\"19_1289418\",\"TransProto\":\"TCP\",\"AppProto\":\"HTTP\",\"TypeStr\":\"text/html\",\"Length\":389,\"Status\":\"REPEAT\",\"LocalUri\":\"/data/csmdp/data/var/http/19/235340\"}\n";
        bean = gson.fromJson(value,Bean.class);
        System.out.println(bean.toString());
    }
}
