package com.dxlau.hadoop.v1.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 执行Reduce端Join.
 *
 * <p>假设包含如下数据集，
 * <li>员工信息(Employee)，包含列（Name, Salary, DeptId），即姓名、薪水、部门ID，对应 `/user/milk/employee/` </li>
 * <li>部门信息(Dept)，包含列（DeptId，Name），即部门ID、部门名称，对应 `/user/milk/dept/` </li>
 *
 * <br>
 * 要求查询员工含部门名称的基础信息，对应SQL：
 * <pre>
 *     select t1.name, t1.salary, t1.dept_id, t2.name
 *     from employee t1 left join dept t2 on t1.dept_id = t2.dept_id;
 *
 *     ----employee----
 *     Bob	    70000	5
 *     Alice	72000	2
 *     Amar	    60000	5
 *     Joe	    55000	5
 *
 *     ----dept----
 *     2	Marking
 *     3	Finance
 *     5	Sales
 * </pre>
 *
 * 运行:
 * <pre>
 *     yarn jar xxx.jar /user/milk/employee/ /user/milk/dept/ /user/milk/out
 * </pre>
 */
public class ReduceJoinJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <left> <right> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Reduce-Join");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        Path out = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, out);
        // 对输出结果不进行压缩
        FileOutputFormat.setCompressOutput(job, false);

        // 设置Mapper\Reducer
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        // 设置输出key/value类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // 若OutPath已存在，运行前先删除
        FileSystem fileSystem = out.getFileSystem(getConf());
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ReduceJoinJob(), args));
    }

    /**
     * Mapper，打标签，根据输入路径或列数量.
     */
    static class JoinMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable outKey = new LongWritable();
        private Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String path = ((FileSplit) context.getInputSplit()).getPath().toString();
            if (path.contains("employee")) {
                // 员工
                String line = value.toString();
                String[] fields = line.split("\t");
                if (fields.length != 3) {
                    // 非法，过滤
                    return;
                }

                outKey.set(Long.parseLong(fields[2]));
                outValue.set("0_" + fields[0] + "_" + fields[1]);

                //map输出
                context.write(outKey, outValue);
            } else if (path.contains("dept")) {
                // 部门
                String line = value.toString();
                String[] fields = line.split("\t");
                if (fields.length != 2) {
                    // 非法，过滤
                    return;
                }

                outKey.set(Long.parseLong(fields[0]));
                outValue.set("1_" + fields[1]);

                context.write(outKey, outValue);
            } else {
                System.out.println("非法输入:" + path);
            }
        }
    }

    /**
     * Reduce，关联左右表.
     */
    static class JoinReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
        private Text outValue = new Text();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> left = new ArrayList<>();
            List<String> right = new ArrayList<>();

            // 构建笛卡尔初始集
            for (Text v : values) {
                String[] fields = v.toString().split("_");
                if (StringUtils.equalsIgnoreCase(fields[0], "0")) {
                    // 左表
                    left.add(v.toString());
                } else if (StringUtils.equalsIgnoreCase(fields[0], "1")) {
                    // 右表
                    right.add(v.toString());
                }
            }

            // 生成笛卡尔积
            for (String l : left) {
                String[] leftFields = l.split("_");
                for (String r : right) {
                    String[] rightFields = r.split("_");

                    // 姓名 部门ID 部门名称 薪水
                    outValue.set(leftFields[1] + "\t" + key.toString() + "\t" + rightFields[1] + "\t" + leftFields[2]);
                    context.write(NullWritable.get(), outValue);
                }
            }
        }
    }
}
