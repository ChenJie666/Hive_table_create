package com.cj.hive;

import com.cj.hive.entity.Tuple;
import com.cj.hive.util.FileUtil;
import com.cj.hive.util.HiveUtil;
import org.apache.tools.ant.util.ReaderInputStream;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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

    @Test
    public void transferFormat1() {
        String transfer_format = FileUtil.readFileFromLocation("transfer_format1");

        String s1 = transfer_format.replaceAll("</.*>", "',");
        String s2 = s1.replaceAll("<(.*)>", "$1 string COMMENT '");
        System.out.println(s2);
    }

    @Test
    public void transferFormat2() throws IOException {
        String transfer_format = FileUtil.readFileFromLocation("transfer_format22");
        String s1 = transfer_format.replaceAll("(\\(.*\\))", "");
        ByteArrayInputStream bais = new ByteArrayInputStream(s1.getBytes());
        InputStreamReader isr = new InputStreamReader(bais);
        BufferedReader br = new BufferedReader(isr);
        String line;
        StringBuffer sb = new StringBuffer();
        while((line = br.readLine())!= null){
            String newLine = line.trim().replaceAll("[\\s]+", " ");
            String[] split = newLine.split(" ");
            String columnName = split[0];
            String comment = split[1];
            sb.append(String.format("%s string COMMENT '%s',\n",columnName,comment));
        }
        System.out.println(sb);
    }

    @Test
    public void test() {
        String transfer_format = FileUtil.readFileFromLocation("test");
        String s1 = transfer_format.replaceAll("</.*>", "',");
        String s2 = s1.replaceAll("<(.*)>(.*)", "$1,");
        System.out.println(s2);
    }

    @Test
    public void test2() throws IOException {
        String test1 = FileUtil.readFileFromLocation("test1");
        String s2 = test1.replaceAll("(.*),", "$1");
        BufferedReader br2 = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(s2.getBytes())));
        String data2;
        ArrayList<String> strings = new ArrayList<>();
        while ((data2 = br2.readLine()) != null){
            strings.add(data2);
        }
        System.out.println(strings);

        String test = FileUtil.readFileFromLocation("test");
        String s1 = test.replaceAll("</.*>", "',");
        String s11 = s1.replaceAll("<(.*)>", "$1 string COMMENT '");
        BufferedReader br1 = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(s11.getBytes())));
        String data1;
        boolean flag;
        StringBuffer sb = new StringBuffer();
        while ((data1 = br1.readLine()) != null){
            flag = false;
            for (String string : strings) {
                if (data1.split(" ")[0].equals(string)) {
                    flag = true;
                    break;
                }
            }
            if (flag) {
                System.out.println(data1);
                sb.append(data1).append('\n');
            }
        }

        System.out.println(sb);
    }

    @Test
    public void customer() throws IOException {
        String customer_comment = FileUtil.readFileFromLocation("customer_comment");
        String customer_comment1 = customer_comment.replaceAll("\\(.*\\)", "");
        String customer_comment2 = customer_comment1.replaceAll(" +", " ");
        BufferedReader commentBR = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(customer_comment2.getBytes())));
        HashMap<String, String> map = new HashMap<>();
        String commentData;
        while ((commentData = commentBR.readLine()) != null) {
            String column = commentData.trim().split(" ")[0];
            String comment = commentData.trim().split(" ")[1];
            map.put(column, comment);
        }
        System.out.println("size:" + map.size());
//        System.out.println(map.entrySet().iterator().next());

        String customer_columns = FileUtil.readFileFromLocation("customer_columns");
        String customer_columns1 = customer_columns.replaceAll(" +", " ");
        BufferedReader columnBR = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(customer_columns1.getBytes())));
        String columnData;
        StringBuffer sb = new StringBuffer();
        boolean flag;
        while ((columnData = columnBR.readLine()) != null) {
            flag = false;
            String column = columnData.trim().split(" ")[0];
            for (Map.Entry<String, String> entry : map.entrySet()) {
//                System.out.println("column:" + column + "  key:" +entry.getKey() + "  value:" + entry.getValue());
                if (column.equalsIgnoreCase(entry.getKey())) {
                    String comment = entry.getValue();
                    String resultColumn = columnData.replaceAll(",", " COMMENT '" + comment + "',");
                    sb.append(resultColumn).append('\n');
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                sb.append(columnData).append('\n');
            }
        }
        System.out.println(sb);
    }

    @Test
    public void edb() throws IOException {
        String edb_comment = FileUtil.readFileFromLocation("edb_comment");
        String edb_comment1 = edb_comment.replaceAll("</.*>", "");
        String edb_comment2 = edb_comment1.replaceAll("<(.*)>", "$1 ");
        System.out.println(edb_comment2);
        BufferedReader commentBR = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(edb_comment2.getBytes())));
        HashMap<String, String> map = new HashMap<>();
        String commentData;
        while ((commentData = commentBR.readLine()) != null) {
            String column = commentData.trim().split(" ")[0];
            String comment = commentData.trim().split(" ")[1];
            map.put(column, comment);
        }
        System.out.println("size:" + map.size());

        String edb_columns = FileUtil.readFileFromLocation("edb_columns");
        String edb_columns1 = edb_columns.replaceAll(" +", " ");
        BufferedReader columnBR = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(edb_columns1.getBytes())));
        String columnData;
        StringBuffer sb = new StringBuffer();
        boolean flag;
        while ((columnData = columnBR.readLine()) != null) {
            flag = false;
            String column = columnData.trim().split(" ")[0];
            for (Map.Entry<String, String> entry : map.entrySet()) {
//                System.out.println("column:" + column + "  key:" +entry.getKey() + "  value:" + entry.getValue());
                if (column.equalsIgnoreCase(entry.getKey())) {
                    String comment = entry.getValue();
                    String resultColumn = columnData.replaceAll(",", " COMMENT '" + comment + "',");
                    sb.append(resultColumn).append('\n');
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                sb.append(columnData).append('\n');
            }
        }
        System.out.println(sb);

    }

}