package com.hadoop.search.pagerank;

import java.util.HashSet;
import java.util.Set;

public  class Count {
    //统计收敛页面数量
    static int count = 0;

    //用于记录网页链接，采用Set集合的方式进行去重，统计网页数量
    static Set<String> webList = new HashSet<String>();

    //记录网页总数
    static int webTotalCount = 0 ;
}

