package com.dxlau.hadoop.utils;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

/**
 * 基于Gson操作JSON
 * Created by dxlau on 2016/11/21.
 */
public final class JsonHelper {

    public static String toJsonString(Object obj) {
        Gson gson = new Gson();
        return gson.toJson(obj);
    }

    public static Map<String, Object> toJsonObj(String jsonStr) {
        Gson gson = new Gson();
        return gson.fromJson(jsonStr, HashMap.class);
    }

    public static void main(String[] args) {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "dxlau");
        map.put("age", 24);

        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("tel", "13111111111");
        innerMap.put("email", "ldxforwork@11.com");
        map.put("link", innerMap);

        String mapJsonStr = toJsonString(map);
        System.out.println(mapJsonStr);

        Map<String, Object> jsonMap = toJsonObj(mapJsonStr);
        System.out.println(jsonMap);
    }
}
