package com.wjj.jdbc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author wangjiajun
 * @Date 2017/8/14 10:47
 */
public class LOG {
    public static Logger getLogger(Class cls){
        return LoggerFactory.getLogger(cls);
    }
}
