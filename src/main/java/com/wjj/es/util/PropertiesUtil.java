package com.wjj.es.util;

import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * Created by wjj on 2017/7/27.
 */
public class PropertiesUtil {
    public static ResourceBundle bundle = ResourceBundle.getBundle("elasticsearch");
    public static String getValue(String key){
        String value = bundle.getString(key);
        return value;
    }
    public static String getValue(String key,String defaultValue){
        String value = getValue(key);
        return StringUtils.isBlank(value)?defaultValue:value;
    }
    public static Map getAll(){
        Enumeration keys =  bundle.getKeys();
        Map result = new HashMap<>();
        while (keys.hasMoreElements()){
            String key = (String) keys.nextElement();
            String value = getValue(key);
            result.put(key,value);
        }
        return result;
    }
}
