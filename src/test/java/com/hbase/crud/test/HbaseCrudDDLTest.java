package com.hbase.crud.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * hbase crud测试（ddl）
 * 
 * @author sanduo
 * @date 2018/10/30
 */
public class HbaseCrudDDLTest {

    private Connection conn;
    private Admin admin;

    @Before
    public void init() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        // 客户端只和zookeeper交互，所以只需要告诉zookeeper的地址（zookeeper-region server）
        // 直接添加，或者添加hbase-site.xml
        configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        // 构建客户端
        conn = ConnectionFactory.createConnection(configuration);
        // 获取一个ddl操作器
        admin = conn.getAdmin();
    }

    // ddl :表的创建删除
    /**
     * 测试创建表
     * 
     * @throws IOException
     */
    @Test
    public void testCreateTable() throws IOException {
        // 定义表的描述信息
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("user_info"));
        // 列族信息
        HColumnDescriptor columnDescriptor_1 = new HColumnDescriptor("base_info");
        // 最大列族版本
        columnDescriptor_1.setMaxVersions(3);
        HColumnDescriptor columnDescriptor_2 = new HColumnDescriptor("extra_info");

        // 将列族定义信息放到表定义信息中
        tableDescriptor.addFamily(columnDescriptor_1);
        tableDescriptor.addFamily(columnDescriptor_2);
        // 创建表
        admin.createTable(tableDescriptor);

    }

    /**
     * 删除表
     * 
     * @throws IOException
     */
    @Test
    public void testDropTable() throws IOException {
        // 停用表
        admin.disableTable(TableName.valueOf("user_info"));
        // 删除表
        admin.deleteTable(TableName.valueOf("user_info"));

    }

    /**
     * 修改表定义
     * 
     * @throws IOException
     */
    @Test
    public void testAlterTable() throws IOException {
        // 获取之前的表定义修改
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("user_info"));
        // 添加新的列族
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("other_info");
        columnDescriptor.setBloomFilterType(BloomType.ROW);// 设置bloom过滤器值按照设置

        // 添加列族定义
        tableDescriptor.addFamily(columnDescriptor);
        // 将修改的提交到admin
        admin.modifyTable(TableName.valueOf("user_info"), tableDescriptor);

    }
    // dml:数据的操作

    // 释放资源
    @After
    public void releaseResource() throws IOException {
        admin.close();
        conn.close();
    }

}
