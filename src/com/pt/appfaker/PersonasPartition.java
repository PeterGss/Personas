package com.pt.appfaker;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PersonasPartition extends Partitioner<Text,Text>{
    @Override
    public int getPartition(Text key, Text value, int reduceNum) {
        double suffix = Math.random() * 31;
        int keyString = (key.toString().hashCode() << 5 & Integer.MAX_VALUE) + (int)suffix;
        return keyString % reduceNum;
    }
}