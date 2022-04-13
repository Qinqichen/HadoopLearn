package com.hadoop.search.pagerank;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

public class Node {
    private double pr;
    private String[] nearbyNodeName;
    private static final char separater = '\t';

    //判断是否有出链
    public boolean havingNode() {
        return (nearbyNodeName != null && nearbyNodeName.length > 0);
    }

    //重写toString方法
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(pr);
        //如果有出链
        if (havingNode()) {
            sb.append(separater);
            sb.append(StringUtils.join(nearbyNodeName, separater));
        }
        return sb.toString();
    }

    /**
     * 如果不是第一次读取，那么就会有pr值，数据的可能是
     * A  1.0
     * A 1.0 B C
     * A 1.0 B
     */
    public static Node getNode(String line) throws Exception {
        String[] str = StringUtils.split(line, separater);
        //如果数组的长度小于1.不符合上面的任何一种情况，所以报错
        if (str.length < 1) {
            throw new Exception("data error");
        }
        Node node = new Node();
        node.setPr(Double.parseDouble(str[0]));
        node.setNearbyNodeName(Arrays.copyOfRange(str, 1, str.length));
        return node;
    }

    /**
     * 如果是第一次读取，那么情况和上面的差不多，就是没有pr值，在这里，我们给其默认值为1
     *
     * @return
     */
    public static Node getNode(String pr, String line) throws Exception {
        return getNode(pr + separater + line);
    }

    public double getPr() {
        return pr;
    }

    public void setPr(double pr) {
        this.pr = pr;
    }

    public String[] getNearbyNodeName() {
        return nearbyNodeName;
    }

    public void setNearbyNodeName(String[] nearbyNodeName) {
        this.nearbyNodeName = nearbyNodeName;
    }

    public char getSeparater() {
        return separater;
    }
}
