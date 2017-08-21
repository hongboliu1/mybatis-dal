package com.bohaisoft.dal.util;

/**
 * Created by ThinkPad on 2016/12/30.
 */
public class DBContextHolder {

    private static final ThreadLocal<String> contextHolder = new ThreadLocal<String>();

    public static void setJdbcType(String jdbcType) {
        contextHolder.set(jdbcType);
    }

    public static void setSlave() {
        setJdbcType("slave");
    }

    public static void setMaster() {
        clearJdbcType();
    }

    public static String getJdbcType() {
        return (String) contextHolder.get();
    }

    public static void clearJdbcType() {
        contextHolder.remove();
    }
}
