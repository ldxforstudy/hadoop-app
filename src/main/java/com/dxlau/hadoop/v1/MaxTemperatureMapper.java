package com.dxlau.hadoop.v1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by dxlau on 16/9/28.
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(15, 19);

        int airTemperature = Integer.parseInt(line.substring(87, 92));
        System.out.println(year + ":" + airTemperature);
        context.write(new Text(year), new IntWritable(airTemperature));
    }
}
