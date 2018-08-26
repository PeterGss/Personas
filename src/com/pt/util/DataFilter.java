package com.pt.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * DataFilter 过滤detaildata和statdata目录，只保留2开头且长度为8的目录，如:20150825
 * @author Administrator
 *
 */
public class DataFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
        String pathName = path.getName();
        MLogger.debug("pathName:" + pathName);
        return pathName != null && pathName.startsWith("2");
    }
}
