package com.pt.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Shaon on 2018/9/27.
 */
public class Test {
    public static void main(String[] args) {
        Set<Bean> set = new HashSet<Bean>();
        Bean bean1 = new Bean("1","1","1","1","1","1","1");
        Bean bean2 = new Bean("1","1","1","1","1","1","2");
        set.add(bean1);
        set.add(bean2);
        System.out.println((new ArrayList<String>()).size());
    }
}
