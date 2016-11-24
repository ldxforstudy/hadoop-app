package com.dxlau.hadoop.autohome;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;


/**
 * PCM端用户的热门车源
 * yarn jar hadoop-app-1.0-UserHotcar.jar /user/hdfs/app/ml/DayTop/topJson_pcm  /user/dxlau/user_hotcar/hotcar  /user/dxlau/user_hotcar/out
 * yarn jar hadoop-app-1.0-UserHotcar.jar /user/dxlau/user_hotcar/profile  /user/dxlau/user_hotcar/hotcar  /user/dxlau/user_hotcar/out
 * Created by dxlau on 2016/11/21.
 */
public class UserHotcar {
    private static final String SPLIT_001_CHAR = "\001";

    static class UserHotcarMapper extends Mapper<LongWritable, Text, Text, Text> {
        enum ValidInputEnum {USER_COUNT, HOTCAR_COUNT}

        private Text outKey = new Text();
        private Text outValue = new Text();
        // 城市-热门车
        Map<String, String> cityHotcarMap = new HashMap<>();

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
//                    System.err.printf("Invalid favPrice: %s\n", favPrice);
                }

                //输出userId:偏好城市\001偏好价格
                StringBuffer outKeyBuf = new StringBuffer();
                StringBuffer outValueBuf = new StringBuffer();
                for (String cityIdScore : cityIdStr.split("$")) {
                    String favCityId = cityIdScore.split("@")[0];
                    String cityHotInfos = cityHotcarMap.get(favCityId);
                    if (cityHotInfos != null) {
                        for (String infoItem : cityHotInfos.split(",")) {
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
                Counter counter = context.getCounter(ValidInputEnum.class.getName(), ValidInputEnum.USER_COUNT.toString());
                counter.increment(1);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            System.out.println("Mapper setup...");
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
//                    System.out.printf("%s has %s\n", cachePath, cacheChildItem.toUri().getPath());
                    try {
                        FSDataInputStream fin = hdfs.open(cacheChildItem);
                        BufferedReader br = new BufferedReader(new InputStreamReader(fin));
                        String line = br.readLine();
                        while (line != null) {
                            String[] userAndInfos = line.split(SPLIT_001_CHAR);
                            if (userAndInfos.length >= 2) {
                                String infoJsonStr = userAndInfos[1];
                                Map<String, Object> infoJson = JsonHelper.toJsonObj(infoJsonStr);
                                String infos = (String) infoJson.get("infoidlist");
                                cityHotcarMap.put(userAndInfos[0], infos);

                                Counter hotcarCounter = context.getCounter(ValidInputEnum.class.getName(), ValidInputEnum.HOTCAR_COUNT.toString());
                                hotcarCounter.increment(1);
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

    static class UserHotcarReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer outKeyBuffer = new StringBuffer();
            outKeyBuffer.append(key.toString());
            outKeyBuffer.append(SPLIT_001_CHAR);

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

            StringBuffer outValueBuf = new StringBuffer();
            for (String infoPriceEle : infoPriceSortSet) {
                String[] infoAndPrice = infoPriceEle.split("@");
                String infoId = infoAndPrice[0];
                Float price = Float.valueOf(infoAndPrice[1]);
                //01标准化
                Float score = 1 - ((price - minNearPrice) / base01);

                outValueBuf.append(infoId);
                outValueBuf.append("@");
                outValueBuf.append(score);
                outValueBuf.append(",");
            }

            String outValue = outValueBuf.substring(0, outValueBuf.length() - 1);
            String jsonContent = "{\"infoids\": \"" + outValue + "\"}";

            outKeyBuffer.append(jsonContent);
            context.write(NullWritable.get(), new Text(outKeyBuffer.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Run UserHotcar");
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] remailArgs = optionsParser.getRemainingArgs();
        if (remailArgs.length != 3) {
            System.err.printf("Usage: %s [generic options] <profile_input> <hotcat_input> <output>\n", UserHotcar.class.getName());
            System.exit(2);
        }

        //1. 创建Job实例
        Job job = Job.getInstance(conf, "dxlau-userhotcar");
        job.setJarByClass(UserHotcar.class);

        //2. 设置Input/Output
        Path profilePath = new Path(remailArgs[0]);
        Path hotcarPath = new Path(remailArgs[1]);
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
        job.setMapperClass(UserHotcarMapper.class);
        job.setReducerClass(UserHotcarReducer.class);
        // 0.95 * node-num(10)
//        job.setNumReduceTasks(9);

        //4. 设置输出key/value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //5. 增加缓存文件
        job.addCacheFile(hotcarPath.toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
