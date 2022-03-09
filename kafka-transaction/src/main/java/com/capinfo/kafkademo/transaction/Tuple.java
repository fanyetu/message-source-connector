package com.capinfo.kafkademo.transaction;

/**
 * @author zhanghaonan
 * @date 2022/2/28
 */
public class Tuple {

    private String key;
    private Integer value;

    private Tuple(String key, Integer value) {
        this.key = key;
        this.value = value;
    }

    public static Tuple of(String key, Integer value) {
        return new Tuple(key, value);
    }

    public String getKey() {
        return key;
    }

    public Integer getValue() {
        return value;
    }
}
