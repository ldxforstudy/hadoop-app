package com.dxlau.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public final class CURDSample {
    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();

        Object x = null;
        Object y = (Object) x;
    }
}
