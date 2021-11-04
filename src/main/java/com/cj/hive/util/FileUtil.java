package com.cj.hive.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

/**
 * @author CJ
 * @date: 2021/11/1 15:57
 */
public class FileUtil {

    public static String readFileFromLocation(String path){
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        assert is != null;
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        return br.lines().collect(Collectors.joining("\n"));
    }

}
