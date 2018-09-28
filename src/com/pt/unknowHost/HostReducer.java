package com.pt.unknowHost;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Shaon on 2018/9/26.
 */
public class HostReducer extends Reducer<Text, Text, Text, Text> {
    public HostReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> set = new HashSet<String>();
        for (Text value : values) {
            set.add(value.toString());
        }
        for (String s : set) {
            context.write(key,new Text(s));
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);
    }
}
