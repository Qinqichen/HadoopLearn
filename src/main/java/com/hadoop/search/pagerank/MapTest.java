package com.hadoop.search.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class MapTest extends Mapper<Text, Text, Text, Text> {
    /**
     * 数据可能是：<A,B   C>  <A,1    B   C>    <A,C>
     *    A B C
     *    A 1 B C
     *    A 1
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        try {
            //投票页面
            String page = key.toString();
            //被投票页面
            String line = value.toString();
            Node node = null;
            //这是第一次运行，pr值是默认值1
            if (context.getConfiguration().getInt("runCount", 1) == 1) {
                node = Node.getNode("1", line);
                //统计页面总个数
                Count.webList.add(page);
                Count.webList.addAll(Arrays.asList(node.getNearbyNodeName()));
                Count.webTotalCount = Count.webList.size();
            } else {
                node = Node.getNode(line);
            }
            //写出投票关系
            context.write(key, new Text(node.toString()));
            //计算投票页面给每个被投票页面的权重
            //先判断是否有出链
            if (node.havingNode()) {
                //有出链，将其pr值平方给出链
                double outPr = node.getPr() / node.getNearbyNodeName().length;
                for (String nearbyNodeName : node.getNearbyNodeName()) {
                    //循环其的每一个出链，并且将其输出
                    context.write(new Text(nearbyNodeName), new Text(String.valueOf(outPr)));
                }
            } else {
                context.write(key, new Text(String.valueOf(node.getPr())));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
