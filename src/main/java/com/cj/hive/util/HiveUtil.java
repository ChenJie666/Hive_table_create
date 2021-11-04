package com.cj.hive.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cj.hive.common.HiveConstant;
import com.cj.hive.entity.Tuple;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author CJ
 * @date: 2021/11/1 16:45
 */
@Slf4j
public class HiveUtil {

    public static Tuple<List<String>, List<String>> getColumn(String fileName) {
        String json = FileUtil.readFileFromLocation(fileName);

        JSONObject jsonObject = JSON.parseObject(json);

        JSONArray tidItemArray = jsonObject.getJSONArray("tid_item");
        JSONObject items = tidItemArray.getJSONObject(0);

        Set<String> jsonKeys = jsonObject.keySet();
        jsonKeys.remove("tid_item");
        ArrayList<String> jsonKeyList = new ArrayList<>(jsonKeys);

        Set<String> itemKeys = items.keySet();
        ArrayList<String> itemKeyList = new ArrayList<>(itemKeys);

        return new Tuple<>(jsonKeyList, itemKeyList);
    }

    public static void createHiveTable(String columns) {

        String fileName = "dwd_table";
        String db = "dw";
        String tableName = "dwd_edb_order";
        String location = "/user/hive/warehouse/dw.db/dwd_edb_order";

        String dwdTable = FileUtil.readFileFromLocation(fileName);

        String ddlSql = String.format(dwdTable, db, tableName, columns, location);

        System.out.println(ddlSql);

        Connection connection = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            connection = DriverManager.getConnection("jdbc:hive2://" + HiveConstant.HIVEHOST + ":"
                    + HiveConstant.HIVEPORT, HiveConstant.USERNAME, HiveConstant.PASSWORD);

            Statement statement = connection.createStatement();
            statement.execute(ddlSql);
        } catch (Exception e) {
            log.error("创建Hive连接失败", e);
            throw new Error("创建Hive连接失败");
        }

        try {
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static String getSelectSql(String columns) {
        String fileName = "parse_json_array";
        String selectSql = FileUtil.readFileFromLocation(fileName);


//        String.format(selectSql, , "hxr_edb", "ods_edb_order", "tid_item");

        return null;
    }

}