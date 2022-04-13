package com.hadoop.search.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class DriTest extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(),new DriTest(),args);
        System.exit(res);

    }


    @Override
    public int run(String[] strings) throws Exception {

        String baseUrl = "hdfs://hadoopqin:9200";
//        String baseUrl = "hdfs://hadoop101:8020";

        Configuration conf = this.getConf();

        conf.set("dfs.client.use.datanode.hostname","true");
//        conf.set("mapreduce.framework.name","yarn");
//
//        conf.set("hadoop.http.staticuser.user","root");

        FileSystem fs = FileSystem.get(new URI(baseUrl), conf, "root");

        String inputPath = baseUrl + "/qin/input/web.txt";
        String outPutPath = baseUrl + "/qin/output/PageRank";

        int runCount = 0;
        while (true) {
            runCount++;
            conf.setInt("runCount", runCount);

            System.out.println("********************* 第" + runCount + "轮执行 **********************");

            Job job = Job.getInstance(conf);
            job.setJarByClass(DriTest.class);

            job.setMapperClass(MapTest.class);
            job.setReducerClass(RedTest.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            /* 设置采用KeyValueTextInputFormat来读取数据封装kv对送到mapper的map方法会根据tab分割数据,把分割后的第一个数据当做key,其它当做value*/
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            Path path = null;
            Path out = null;
            if (runCount == 1) {
                //第一次运行
                path = new Path(inputPath);
            } else {
                path = new Path(outPutPath + "/part" + (runCount - 1));
            }
            FileInputFormat.setInputPaths(job, path);
            out = new Path(outPutPath + "/part" + runCount);
            if (out.getFileSystem(conf).exists(out)) {
                out.getFileSystem(conf).delete(out, true);
            }
            FileOutputFormat.setOutputPath(job, out);
            boolean b = job.waitForCompletion(true);
            if (b) {
                if (Count.count >= 4) {
                    break;
                }
            }
            Count.count = 0 ;
        }

        return 0;
    }
}