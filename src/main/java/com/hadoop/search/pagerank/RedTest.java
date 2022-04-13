package com.hadoop.search.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RedTest extends Reducer<Text, Text, Text, Text> {
    /**
     * 这里的数据类型可能有如下几种
     * A,1  B   C
     * A,0.5
     * A,0.5
     * 第一种是他的页面关系
     * 第二三种是这个页面被赋予的pr
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        try {
            Node orinode = null;
            double sum = 0.0;
            //先判断数据是哪一种
            for (Text v : values) {
                Node node = Node.getNode(v.toString());
                //如果有临近的链，那么就是第一种数据
                if (node.havingNode()) {
                    orinode = node;
                } else {
                    sum += node.getPr();
                }
            }
            //求出新的pr值
            double newPr = (1 - 0.85) / Count.webTotalCount + 0.85 * (sum/orinode.getNearbyNodeName().length);
            //求差值,因为是差值所以可以换成绝对值，好比较
            double diff = Math.abs(orinode.getPr() - newPr);
            //将新的pr值赋给node
            orinode.setPr(newPr);
            System.out.println();
            //如果差值控制在百分位代表稳定
            if (diff < 0.001) {
                //计算已经收敛的数量
                Count.count = Count.count+1;
            }
            context.write(key, new Text(orinode.toString()));
            System.out.println(key+"的pr值:"+orinode.getPr());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

