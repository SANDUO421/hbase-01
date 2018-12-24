package com.hbase.crud.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * hbase crud测试（dml）
 * 
 * @author sanduo
 * @date 2018/10/30
 */
public class HbaseCrudDMLTest {

    private Connection conn;

    private Table table;

    @Before
    public void init() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        // 客户端只和zookeeper交互，所以只需要告诉zookeeper的地址（zookeeper-region server）
        // 直接添加，或者添加hbase-site.xml
        // configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        // 构建客户端
        conn = ConnectionFactory.createConnection(configuration);
        // 获取一个操作指定表table对象，进行dml操作
        table = conn.getTable(TableName.valueOf("user_info"));
    }

    // dml:数据的操作

    /**
     * 增
     * 
     * 改：put覆盖
     * 
     * @throws IOException
     */
    @Test
    public void testAdd() throws IOException {
        // 指定行键--封装数据（一个put 对应一个rowkey）
        Put put = new Put(Bytes.toBytes("001"));

        put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("张三"));
        put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes(18));
        put.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("addr"), Bytes.toBytes("西安"));

        Put put2 = new Put(Bytes.toBytes("002"));

        put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("王八"));
        put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
        put2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("addr"), Bytes.toBytes("北京"));

        List<Put> list = new ArrayList<Put>();
        list.add(put);
        list.add(put2);

        table.put(list);

    }

    /**
     * 批量增加
     * 
     * @throws IOException
     */
    @Test
    public void testAddMany() throws IOException {
        List<Put> list = new ArrayList<Put>();
        for (int i = 0; i < 1000; i++) {
            Put put = new Put(Bytes.toBytes("" + i));

            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("张三" + i));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes(18));
            put.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("addr"), Bytes.toBytes("西安"));
            list.add(put);
        }
        table.put(list);
    }

    /**
     * 删
     * 
     * @throws IOException
     */
    @Test
    public void testDelete() throws IOException {

        // 删除001
        Delete d1 = new Delete(Bytes.toBytes("001"));
        // 删除002 中的extra_info
        Delete d2 = new Delete(Bytes.toBytes("002"));
        d2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("addr"));

        ArrayList<Delete> list = new ArrayList<Delete>();
        list.add(d1);
        list.add(d2);
        table.delete(list);
    }

    /**
     * 查 count 'user_info'
     * 
     * @throws IOException
     */
    @Test
    public void testGet() throws IOException {
        // 获取行键
        Get get = new Get(Bytes.toBytes("002"));
        Result result = table.get(get);
        // 从结果中去某个指定的值
        byte[] age = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("age"));
        System.out.println("年龄：" + new String(age));
        // 读取数据,遍历正行中的所有单元格
        CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
            // 获取单元格的值
            Cell cell = cellScanner.current();
            byte[] rowArray = cell.getRowArray();// 行键
            byte[] familyArray = cell.getFamilyArray();// 列族
            byte[] qualifierArray = cell.getQualifierArray();// 键名
            byte[] valueArray = cell.getValueArray();// 键值
            System.out.println("行键：" + new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
            System.out.println("列族：" + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyOffset()));
            System.out
                .println("键名：" + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
            System.out.println("键值：" + new String(valueArray, cell.getValueOffset(), cell.getValueLength()));
        }

    }

    /**
     * 按照行键范围查询
     * 
     * @throws IOException
     */
    @Test
    public void testScan() throws IOException {
        // 包含其实，不包含结束;只有在1000后面拼一个，才可以查到1000，否则不包含Bytes.toBytes("1000\00")
        // 拼接一个不可见的字符\000
        Scan scan = new Scan(Bytes.toBytes("10"), Bytes.toBytes("1000\000"));
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            // 从结果中去某个指定的值
            byte[] age = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("age"));
            System.out.println("年龄：" + new String(age));
            // 读取数据,遍历正行中的所有单元格
            CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()) {
                // 获取单元格的值
                Cell cell = cellScanner.current();
                byte[] rowArray = cell.getRowArray();// 行键
                byte[] familyArray = cell.getFamilyArray();// 列族
                byte[] qualifierArray = cell.getQualifierArray();// 键名
                byte[] valueArray = cell.getValueArray();// 键值
                System.out.println("行键：" + new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
                System.out.println("列族：" + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyOffset()));
                System.out
                    .println("键名：" + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
                System.out.println("键值：" + new String(valueArray, cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("----------");
        }
    }

    // 释放资源
    @After
    public void releaseResource() throws IOException {
        table.close();
        conn.close();
    }

}
