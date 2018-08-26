package com.pt.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 *  InputDataFilter 时间过滤器，只保留文件目录名大于等于mindate的目录
 * @author Administrator
 *
 */
public class InputDataFilter implements PathFilter {

    String minDate;
    String maxDate;
    
    /**
     * 过滤 闭区间
     * @param minDate
     * @param maxDate
     */
    public InputDataFilter(String minDate, String maxDate) {
        this.minDate = minDate;
        this.maxDate = maxDate;
    }

    @Override
    public boolean accept(Path path) {
        String name = path.getName();
        return name != null && name.compareToIgnoreCase(minDate) >= 0
                && name.compareToIgnoreCase(maxDate) < 0;
    }
}