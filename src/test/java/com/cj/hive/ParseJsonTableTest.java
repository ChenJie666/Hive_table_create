package com.cj.hive;

import com.cj.hive.entity.Tuple;
import com.cj.hive.util.FileUtil;
import com.cj.hive.util.HiveUtil;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * @author CJ
 * @date: 2021/11/1 17:26
 */
public class ParseJsonTableTest {

    /**
     * 创建表
     */
    @Test
    public void createTable() {
        Tuple<List<String>, List<String>> edbSalesColumn = HiveUtil.getColumn("order_json");

        List<String> jsonKeys = edbSalesColumn.getFirst();
        List<String> itemKeys = edbSalesColumn.getSecond();

        ArrayList<String> keyList = new ArrayList<>(jsonKeys);
        ArrayList<String> itemList = new ArrayList<>();

        itemKeys.forEach(item -> itemList.add("item_" + item));
        keyList.addAll(itemList);

        StringBuffer columnStr = new StringBuffer();
        Iterator<String> itemIter = keyList.iterator();
        while (itemIter.hasNext()) {
            String itemKey = itemIter.next();
            columnStr.append(String.format("%s string,", itemKey));
        }
        columnStr.deleteCharAt(columnStr.length() - 1);

        System.out.println(columnStr);

//        String columns = String.join(keyList, " string,") + " string";
//        System.out.println(columns);

        HiveUtil.createHiveTable(columnStr.toString());
    }

    /**
     * 测试每次字段排序是否是相同的
     */
    @Test
    public void testSeq() {
        long starttime = System.currentTimeMillis();

        ArrayList<CompletableFuture<Integer>> tasks = new ArrayList<>();
        ArrayList<Integer> results = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            CompletableFuture<Integer> integerCompletableFuture = CompletableFuture.supplyAsync(() -> {
                Tuple<List<String>, List<String>> edbSalesColumn = HiveUtil.getColumn("order_json");

                ArrayList<String> keyList = new ArrayList<>();
                keyList.addAll(edbSalesColumn.getFirst());
                keyList.addAll(edbSalesColumn.getSecond());

                return keyList.hashCode();
            });
            tasks.add(integerCompletableFuture);
        }

        tasks.forEach(task -> {
            try {
                results.add(task.get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        System.out.println(results);


        long endtime = System.currentTimeMillis();
        System.out.println(endtime - starttime);
    }

    /**
     * 获取解析json_array的hql语句
     */
    @Test
    public void printSelectSql() {
        Tuple<List<String>, List<String>> edbSalesColumn = HiveUtil.getColumn("order_json");

        List<String> jsonKeys = edbSalesColumn.getFirst();
        List<String> itemKeys = edbSalesColumn.getSecond();

        StringBuffer sb = new StringBuffer();

        Iterator<String> jsonIter = jsonKeys.iterator();
        while (jsonIter.hasNext()) {
            String jsonKey = jsonIter.next();
            sb.append(String.format("get_json_object(line, '$.%s'),", jsonKey));
        }

        Iterator<String> itemIter = itemKeys.iterator();
        while (itemIter.hasNext()) {
            String itemKey = itemIter.next();
            sb.append(String.format("get_json_object(item, '$.%s'),", itemKey));
        }
        sb.deleteCharAt(sb.length() - 1);


        String parseJson = FileUtil.readFileFromLocation("parse_json_array");
        String sql = String.format(parseJson, sb, "hxr_edb", "ods_edb_order", "tid_item");

        System.out.println(sql);
    }

//    @Test
//    public void test() throws ExecutionException, InterruptedException {
//
//        CompletableFuture<Integer> integerOldCompletableFuture = CompletableFuture.supplyAsync(() -> {
//            Tuple<Set<String>, Set<String>> edbSalesColumn = HiveUtil.getColumn("json_example");
//
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//            ArrayList<String> keyList = new ArrayList<>();
//            keyList.addAll(edbSalesColumn.getFirst());
//            keyList.addAll(edbSalesColumn.getSecond());
//
//            return keyList.hashCode();
//        });
//
//        CompletableFuture<Integer> integerNewCompletableFuture = CompletableFuture.supplyAsync(() -> {
//            Tuple<Set<String>, Set<String>> edbSalesColumn = HiveUtil.getColumn("order_json");
//
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//            ArrayList<String> keyList = new ArrayList<>();
//            keyList.addAll(edbSalesColumn.getFirst());
//            keyList.addAll(edbSalesColumn.getSecond());
//
//            return keyList.hashCode();
//        });
//
//        Integer oldHash = integerOldCompletableFuture.get();
//        Integer newHash = integerNewCompletableFuture.get();
//
//        System.out.println("old: " + oldHash);
//        System.out.println("new: " + newHash);
//
//    }

}