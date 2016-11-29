package com.dxlau.hadoop.utils;


import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Calendar;

/**
 * 基于JDK8日期时间API提供
 * Created by dxlau on 2016/11/29.
 */
public final class DateHelper {

    public static final String FULL_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String DATE_FORMAT = "yyyy-MM-dd";

    public static DateTime parseDateTime(String dateStr, String format) {
        DateTimeFormatter dtf;
        if (StringUtils.isEmpty(format)) {
            dtf = DateTimeFormat.forPattern(FULL_FORMAT);
        } else {
            dtf = DateTimeFormat.forPattern(format);
        }
        DateTime dateTime = dtf.parseDateTime(dateStr);
        return dateTime;
    }

    /**
     * 操作时间日期,前移或后移
     *
     * @param from   原始日期
     * @param unit   时间单位,可以从Calender中取
     * @param offset 偏移量
     * @return
     */
    public static DateTime offsetDateTime(DateTime from, int unit, int offset) {
        DateTime resDateTime = null;
        if (unit == Calendar.DAY_OF_YEAR) {
            resDateTime = from.plusDays(offset);
        } else if (unit == Calendar.MONTH) {
            resDateTime = from.plusMonths(offset);
        }
        return resDateTime;
    }

    /**
     * 第一秒
     *
     * @param from
     * @return
     */
    public static DateTime get1stSecondOfDays(DateTime from) {
        DateTime resDateTime = new DateTime(from.getYear(), from.getMonthOfYear(), from.getDayOfMonth(), 0, 0);
        return resDateTime;
    }

    /**
     * 最后一秒
     *
     * @param from
     * @return
     */
    public static DateTime getLastSecondOfDays(DateTime from) {
        DateTime resDateTime = new DateTime(from.getYear(), from.getMonthOfYear(), from.getDayOfMonth(), 23, 59, 59);
        return resDateTime;
    }

    /**
     * 返回yyyy-MM-dd格式字符串
     * @param from
     * @return
     */
    public static String toDateStr(DateTime from){
        DateTimeFormatter dtf = DateTimeFormat.forPattern(DATE_FORMAT);
        return dtf.print(from);
    }

    public static void main(String[] args) {
        System.out.println(parseDateTime("2016-07-04 15:01:27.34", ""));
        System.out.println(get1stSecondOfDays(new DateTime()));
        System.out.println(getLastSecondOfDays(new DateTime()));
        System.out.println(toDateStr(DateHelper.offsetDateTime(new DateTime(), Calendar.DAY_OF_YEAR, -1)));
    }
}
