package com.dxlau.hadoop.autohome;

import com.dxlau.hadoop.utils.DateHelper;
import com.dxlau.hadoop.utils.JsonHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.joda.time.DateTime;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * PCM端用户的偏好新车
 * yarn jar hadoop-app-1.0-UserNewcar.jar /user/hdfs/app/ml/DayTop/topJson_pcm  /user/hdfs/app/ml/CityNewcar  /user/hdfs/app/ml/FavNewcar
 * Created by dxlau on 2016/11/29.
 */
public class UserNewcar {
    private static final Integer NEWCAR_COUNT = 300;
    private static final String SPLIT_001_CHAR = "\001";
    private static final int TOP_60 = 60;

    static class UserNewcarMapper extends Mapper<LongWritable, Text, Text, Text> {
        enum ValidInputEnum {USER_COUNT, NEWCAR_COUNT, MORE_112_COLS, OUTSITE, UN_PUB, NONSELL, NOT_NEW, EXCEPTION}

        private Text outKey = new Text();
        private Text outValue = new Text();
        private Map<String, String> cityNewInfosMap = new HashMap<>(NEWCAR_COUNT);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String plainText = value.toString();
            String[] splitArr = plainText.split(SPLIT_001_CHAR);
            if (splitArr.length >= 2) {
                String userId = splitArr[0];
                String userProfile = splitArr[1];
                Map<String, Object> profileMap = JsonHelper.toJsonObj(userProfile);
                Map<String, Object> bycarProfile = (Map<String, Object>) profileMap.get("bycar_profile");

                //偏好城市
                String cityIdStr = (String) bycarProfile.get("cityid");
                //偏好价格,购买力
                String favPrice = (String) bycarProfile.get("priceid");
                Float favPriceFloat = 0.0F;
                try {
                    favPriceFloat = Float.parseFloat(favPrice);
                } catch (Exception e) {
                }

                //output: userId\001车源ID@偏好价格
                StringBuffer outKeyBuf = new StringBuffer();
                StringBuffer outValueBuf = new StringBuffer();

                for (String cityIdScore : cityIdStr.split("$")) {
                    String favCityId = cityIdScore.split("@")[0];
                    String cityNewInfos = cityNewInfosMap.get(favCityId);
                    if (cityNewInfos != null) {
                        for (String infoItem : cityNewInfos.split(",")) {
                            String[] infoAndPrice = infoItem.split("@");
                            String infoId = infoAndPrice[0];
                            Float infoPrice = Float.valueOf(infoAndPrice[1]);

                            // 用户购买力与车源价格的近似值
                            Float favInfoPrice = Math.abs(favPriceFloat - infoPrice);

                            outKeyBuf.append(userId);
                            outKeyBuf.append("_");
                            outKeyBuf.append(favCityId);

                            outValueBuf.append(infoId);
                            outValueBuf.append("@");
                            outValueBuf.append(favInfoPrice);

                            outKey.set(outKeyBuf.toString());
                            outValue.set(outValueBuf.toString());

                            //map输出,并清空buf
                            context.write(outKey, outValue);
                            outKeyBuf.setLength(0);
                            outValueBuf.setLength(0);
                        }
                    }
                }
            }
            Counter counter = context.getCounter(ValidInputEnum.class.getName(), ValidInputEnum.USER_COUNT.toString());
            counter.increment(1);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            FileSystem hdfs = FileSystem.get(context.getConfiguration());
            for (URI cacheItem : cacheFiles) {
                Path cachePath = new Path(cacheItem);
                if (!hdfs.isDirectory(cachePath)) {
                    // 非目录就跳过
                    continue;
                }

                FileStatus[] cacheFileStatus = hdfs.listStatus(cachePath);
                Path[] cacheChildPaths = FileUtil.stat2Paths(cacheFileStatus);
                for (Path cacheChildItem : cacheChildPaths) {
                    try {
                        if (hdfs.isDirectory(cacheChildItem)) {
                            continue;
                        }
                        FSDataInputStream fin = hdfs.open(cacheChildItem);
                        BufferedReader br = new BufferedReader(new InputStreamReader(fin));
                        String line = br.readLine();
                        while (line != null) {
                            String[] userAndInfos = line.split(SPLIT_001_CHAR);
                            if (userAndInfos.length >= 2) {
                                String infoJsonStr = userAndInfos[1];
                                Map<String, Object> infoJson = JsonHelper.toJsonObj(infoJsonStr);
                                String infos = (String) infoJson.get("infoidlist");
                                cityNewInfosMap.put(userAndInfos[0], infos);

                                Counter newcarCounter = context.getCounter(ValidInputEnum.class.getName(), ValidInputEnum.NEWCAR_COUNT.toString());
                                newcarCounter.increment(1);
                            }
                            line = br.readLine();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.err.println("读取缓存文件出错: " + cacheChildItem.getName());
                    }
                }
            }
        }
    }

    static class UserNewcarReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer finalValueBuffer = new StringBuffer();
            finalValueBuffer.append(key.toString());
            finalValueBuffer.append(SPLIT_001_CHAR);

            TreeSet<String> infoPriceSortSet = new TreeSet<>(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    Float o1Price = Float.parseFloat(o1.split("@")[1]);
                    Float o2Price = Float.parseFloat(o2.split("@")[1]);
                    if (o1Price > o2Price) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            });
            for (Text value : values) {
                infoPriceSortSet.add(value.toString());
            }
            String maxNearPriceEle = infoPriceSortSet.last();
            String minNeadPriceEle = infoPriceSortSet.first();
            Float maxNearPrice = Float.valueOf(maxNearPriceEle.split("@")[1]);
            Float minNearPrice = Float.valueOf(minNeadPriceEle.split("@")[1]);
            Float base01 = maxNearPrice - minNearPrice;

            int count = 0;
            StringBuffer outContentBuf = new StringBuffer();
            for (String infoPriceEle : infoPriceSortSet) {
                //在这里已经是偏好价格了,取前100
                if (count >= TOP_60) {
                    break;
                }
                String[] infoAndPrice = infoPriceEle.split("@");
                String infoId = infoAndPrice[0];
                Float price = Float.valueOf(infoAndPrice[1]);
                //01标准化
                Float score = 1 - ((price - minNearPrice) / base01);

                outContentBuf.append(infoId);
                outContentBuf.append("@");
                outContentBuf.append(score);
                outContentBuf.append(",");
                count++;
            }

            String outValue = outContentBuf.substring(0, outContentBuf.length() - 1);
            String jsonContent = "{\"infoids\": \"" + outValue + "\"}";

            finalValueBuffer.append(jsonContent);
            context.write(NullWritable.get(), new Text(finalValueBuffer.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Run UserNewcar");
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] remailArgs = optionsParser.getRemainingArgs();
        if (remailArgs.length != 3) {
            System.err.printf("Usage: %s [generic options] <profile_input> <newcar_input> <output>\n", UserNewcar.class.getName());
            System.exit(2);
        }

        //1. 创建Job实例
        Job job = Job.getInstance(conf, "dxlau-usernewcar");
        job.setJarByClass(UserHotcar.class);

        //2. 设置Input/Output
        Path profilePath = new Path(remailArgs[0]);
        Path outputPath = new Path(remailArgs[2]);

        FileSystem outputPathFs = outputPath.getFileSystem(conf);
        if (outputPathFs.exists(outputPath)) {
            outputPathFs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, profilePath);
        FileOutputFormat.setOutputPath(job, outputPath);
        // 对输出结果不进行压缩
        FileOutputFormat.setCompressOutput(job, false);

        //3. 设置mapper/reducer
        job.setMapperClass(UserNewcarMapper.class);
        job.setReducerClass(UserNewcarReducer.class);
        // 0.95 * node-num(10)
        job.setNumReduceTasks(9);

        //4. 设置输出key/value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //5. 增加缓存文件
        String yesterdayDateStr = DateHelper.toDateStr(DateHelper.offsetDateTime(new DateTime(), Calendar.DAY_OF_YEAR, -1));
        String cacheParentPath = remailArgs[1];
        String cachePath = cacheParentPath + "/dt=" + yesterdayDateStr;
        System.out.println("待缓存路径: " + cachePath);

        Path newcarPath = new Path(cachePath);
        job.addCacheFile(newcarPath.toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
