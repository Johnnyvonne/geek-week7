package org.geekbang.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class StudentTest {

    public static void main(String[] args) throws IOException {
        // 建立连接
        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.quorum", "emr-worker-2,emr-worker-1,emr-header-1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:60000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();
        NamespaceDescriptor.Builder hbase1 = NamespaceDescriptor.create("zanglei");
        //设置namespace的属性信息
        // 构造器对象调用构建方法,返回/获取构建对象
        NamespaceDescriptor build = hbase1.build();
        //创建名称空间
        admin.createNamespace(build);

        TableName tableName = TableName.valueOf("zanglei:student");
        String colFamily1 = "info";
        String colFamily2 = "score";
        int rowKey = 1;

        // 建表
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor(colFamily1);
            HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(colFamily2);
            hTableDescriptor.addFamily(hColumnDescriptor1);
            hTableDescriptor.addFamily(hColumnDescriptor2);

            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }

        // 插入数据
        Put tom = new Put(Bytes.toBytes("Tom")); // row key
        tom.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000001"));
        tom.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes("1"));
        tom.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes("75"));
        tom.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes("82"));
        conn.getTable(tableName).put(tom);
        System.out.println("Data tom insert success");

        Put jerry = new Put(Bytes.toBytes("Jerry")); // row key
        jerry.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000002"));
        jerry.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes("1"));
        jerry.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes("85"));
        jerry.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes("67"));
        conn.getTable(tableName).put(jerry);
        System.out.println("Data Jerry insert success");

        Put jack = new Put(Bytes.toBytes("Jack")); // row key
        jack.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000003"));
        jack.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes("2"));
        jack.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes("80"));
        jack.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes("80"));
        conn.getTable(tableName).put(jack);
        System.out.println("Data jack insert success");

        Put rose = new Put(Bytes.toBytes("Rose")); // row key
        rose.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000004"));
        rose.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes("2"));
        rose.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes("60"));
        rose.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes("61"));
        conn.getTable(tableName).put(rose);
        System.out.println("Data rose insert success");

        Put zanglei = new Put(Bytes.toBytes("臧磊")); // row key
        zanglei.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes("G20210698030125"));
        zanglei.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes("3"));
        zanglei.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes("100"));
        zanglei.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes("100"));
        conn.getTable(tableName).put(zanglei);
        System.out.println("Data 臧磊 insert success");

        // 查看数据
        Get get = new Get(Bytes.toBytes("臧磊"));
        select(get, conn, tableName);

        Put to_delete = new Put(Bytes.toBytes("delete")); // row key
        to_delete.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("student_id"), Bytes.toBytes("123456"));
        to_delete.addColumn(Bytes.toBytes(colFamily1), Bytes.toBytes("class"), Bytes.toBytes("3"));
        to_delete.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes("0"));
        to_delete.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes("0"));
        conn.getTable(tableName).put(to_delete);
        System.out.println("Data to_delete insert success");

        Get getTodelete = new Get(Bytes.toBytes("delete"));
        select(getTodelete, conn, tableName);

        // 删除数据
        Delete delete = new Delete(Bytes.toBytes("delete"));      // 指定rowKey
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");

        select(getTodelete, conn, tableName);
    }

    private static void select(Get get, Connection conn, TableName tableName) throws IOException {
        if (!get.isCheckExistenceOnly()) {
            System.out.println("isCheckExistenceOnly() true");
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        } else {
            System.out.println("isCheckExistenceOnly() false");
        }
    }
}
