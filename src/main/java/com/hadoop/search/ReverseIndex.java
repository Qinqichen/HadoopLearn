package com.hadoop.search;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

public class ReverseIndex {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        protected void map(
                LongWritable key,
                Text value,
                org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            String[] data = value.toString().split(" ");
            //FileSplit类从context上下文中得到，可以获得当前读取的文件的路径
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            //文件路径为hdfs://hadoop:9000/ii/a.txt
            //根据/分割取最后一块即可得到当前的文件名
            System.out.println(fileSplit.getPath().toString());
            String[] fileNames = fileSplit.getPath().toString().split("/");
            String fileName = fileNames[fileNames.length - 1];
            for (String d : data) {
                k.set(d + "->" + fileName);
                v.set("1");
                context.write(k, v);
            }
        };
    }



    public static class MyCombiner extends Reducer<Text, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        protected void reduce(
                Text key,
                java.lang.Iterable<Text> values,
                org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            //分割文件名和单词
            String[] wordAndPath = key.toString().split("->");
            //统计出现次数
            int counts = 0;
            for (Text t : values) {
                counts += Integer.parseInt(t.toString());
            }
            //组成新的key-value输出
            k.set(wordAndPath[0]);
            v.set(wordAndPath[1] + "->" + counts);
            context.write(k, v);
        };
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        private Text v = new Text();

        protected void reduce(
                Text key,
                java.lang.Iterable<Text> values,
                org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            String res = "";
            for (Text text : values) {
                res += text.toString() + "\r";
            }
            v.set(res);
            context.write(key, v);
        };
    }

    public static void main(String[] args) throws Exception {

        String baseUrl = "hdfs://hadoopqin:9200";

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI(baseUrl),conf,"root");
        Path inPath = new Path(baseUrl+"/qin/novels");
        Path outPath = new Path(baseUrl+ "/qin/output/");

        fs.create(new Path("/test/"));

        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(ReverseIndex.class);

        FileInputFormat.setInputPaths(job, inPath);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(MyCombiner.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, outPath);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);
    }

}
