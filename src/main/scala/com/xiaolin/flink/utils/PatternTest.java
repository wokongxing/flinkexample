package com.xiaolin.flink.utils;

import java.util.regex.Pattern;

/**
 * @program: flink-example
 * @description: 正则匹配测试
 * @author: linzy
 * @create: 2020-08-06 17:41
 **/
public class PatternTest {
    public static void main(String[] args) {
        pattertest();
    }

    private static void  pattertest(){
        Pattern   pattern = Pattern.compile("ssckafka\\d");

        System.out.println( pattern.matcher("sscafka1").matches());


    }
}
