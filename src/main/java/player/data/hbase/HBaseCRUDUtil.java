package player.data.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * HBASE CRUD 工具类
 * <p>
 * Java 客户端API实现
 * <p>
 * todo 换成连接池
 * Created by yaoo on 2/15/18
 */
@SuppressWarnings("unused")
public class HBaseCRUDUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseCRUDUtil.class);

    private Configuration configuration;

    public HBaseCRUDUtil(String[][] arrKV) {

        configuration = HBaseConfiguration.create();
        for (String[] kv : arrKV) {
            configuration.set(kv[0], kv[1]);
        }

    }

    public String[] listAllTables() {

        String[] resultTables;

        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();

            HTableDescriptor[] tableDescriptor = admin.listTables();

            resultTables = new String[tableDescriptor.length];

            for (int i = 0; i < tableDescriptor.length; i++) {
                resultTables[i] = tableDescriptor[i].getNameAsString();
            }

            admin.close();
            connection.close();

            return resultTables;

        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
        return null;
    }

    /**
     * 根据行键查询一行
     */
    public void queryByRowkey(String strTableName, String strRowkey) {

        TableName tableName = TableName.valueOf(strTableName);

        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(tableName);

            Get scan = new Get(strRowkey.getBytes());
            Result result = table.get(scan);
            LOG.info("rowkey是: " + new String(result.getRow()));

            for (Cell rowKV : result.rawCells()) {
//                System.out.print("行键是: " + new String(CellUtil.cloneRow(rowKV)) + " ");
                System.out.print("列族名: " + new String(CellUtil.cloneFamily(rowKV)) + " ");
                System.out.print("列名是:  " + new String(CellUtil.cloneQualifier(rowKV)) + " ");
                System.out.print("列值是: " + new String(CellUtil.cloneValue(rowKV)) + " ");
                LOG.info("时间戳: " + rowKV.getTimestamp() + " ");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据列值进行严格查询
     */
    public void queryByColumnValue(String strTableName, String strCF, String strQualifier, String strValue) {
        TableName tableName = TableName.valueOf(strTableName);

        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(tableName);
            // 当列column1的值为aaa时进行查询
            Filter filter = new SingleColumnValueFilter(strCF.getBytes(), strQualifier.getBytes(), CompareFilter.CompareOp.EQUAL, strValue.getBytes());
            Scan s = new Scan();
            s.setFilter(filter);
            ResultScanner rs = table.getScanner(s);

            for (Result result : rs) {
                for (Cell rowKV : result.rawCells()) {
//                    System.out.print("行键是: " + new String(CellUtil.cloneRow(rowKV)) + " ");
                    System.out.print("列族名: " + new String(CellUtil.cloneFamily(rowKV)) + " ");
                    System.out.print("列名是:  " + new String(CellUtil.cloneQualifier(rowKV)) + " ");
                    System.out.print("列值是: " + new String(CellUtil.cloneValue(rowKV)) + " ");
//                    LOG.info("时间戳: " + rowKV.getTimestamp() + " ");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 根据值对多个列进行查询
     */
    public void queryByMultiColumns(String strTableName, String[] strCFs, String[] strQualifers, String[] strValues) {
        TableName tableName = TableName.valueOf(strTableName);

        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(tableName);

            List<Filter> filters = new ArrayList<Filter>();

            for (int i = 0; i < strQualifers.length; i++) {
                Filter f = new SingleColumnValueFilter(strCFs[i].getBytes(), strQualifers[i].getBytes(), CompareFilter.CompareOp.EQUAL, strValues[i].getBytes());
                filters.add(f);
            }

            FilterList filterList = new FilterList(filters);

            Scan scan = new Scan();
            scan.setFilter(filterList);
            ResultScanner rs = table.getScanner(scan);

            for (Result result : rs) {
                for (Cell rowKV : result.rawCells()) {
                    System.out.print("行键是: " + new String(CellUtil.cloneRow(rowKV)) + " ");
                    System.out.print("列族名: " + new String(CellUtil.cloneFamily(rowKV)) + " ");
                    System.out.print("列名是:  " + new String(CellUtil.cloneQualifier(rowKV)) + " ");
                    System.out.print("列值为: " + new String(CellUtil.cloneValue(rowKV)) + " ");
                    LOG.info("时间戳: " + rowKV.getTimestamp() + " ");
                }
            }

            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 打印表所有行的方法，本质是scan
     */
    public void queryAll(String strTableName) {
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(strTableName));
            ResultScanner rs = table.getScanner(new Scan());
            //TO-DO:表为空时要提示
            for (Result result : rs) {
                for (Cell rowKV : result.rawCells()) {
                    System.out.print("行键为: " + new String(CellUtil.cloneRow(rowKV)) + " ");
                    System.out.print("列族名: " + new String(CellUtil.cloneFamily(rowKV)) + " ");
                    System.out.print("列名是:  " + new String(CellUtil.cloneQualifier(rowKV)) + " ");
                    System.out.print("列值是: " + new String(CellUtil.cloneValue(rowKV)) + " ");
                    LOG.info("时间戳: " + rowKV.getTimestamp() + " ");
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // 表的插入操作

    /**
     * 插入键值对
     *
     * @param strTableName    表名
     * @param strRowkey       行键
     * @param strColumnFamily 列族名
     * @param strQualifier    列名
     * @param strValue        值名
     */
    //TO-DO: HBase RegionServer 挂掉异常处理
    public void insertRow(String strTableName, String strRowkey, String strColumnFamily, List<String> strQualifier, List<String> strValue) {
//        LOG.info("=====开始插入数据到"+strTableName+"=====");
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(strTableName));

            // 一个PUT代表一行数据
            Put put = new Put(strRowkey.getBytes());

            //一次添加一个单元格的kv
            //put.addColumn(strColumnFamily.getBytes(), strQualifier.getBytes(), strValue.getBytes());

            //一次添加一行数据，多个kv
            for (int i = 0; i < strQualifier.size(); i++) {
                //列名为空的单独处理
                if (strQualifier.get(i) == null || strQualifier.get(i).isEmpty()) {
                    put.addColumn(strColumnFamily.getBytes(), "".getBytes(), strValue.get(i).getBytes());
                } else {
                    put.addColumn(strColumnFamily.getBytes(), strQualifier.get(i).getBytes(), strValue.get(i).getBytes());
                }
            }
            table.put(put);
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("=====插入数据结束=====");
    }


    // 表的删除操作
    public void deleteTable(String strTableName) {
        TableName tableName = TableName.valueOf(strTableName);
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin hBaseAdmin = connection.getAdmin();

            if (hBaseAdmin.tableExists(tableName)) {
                hBaseAdmin.disableTable(tableName);
                hBaseAdmin.deleteTable(tableName);
                LOG.info("=====表 " + strTableName + " 已删除完成");
            } else {
                LOG.info("=====所要删除的表：" + strTableName + "不存在，请检查表名");
                return;
            }

            hBaseAdmin.close();
            connection.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteRowsByRowkeys(String strTableName, String[] strRowkeys) {
        TableName tableName = TableName.valueOf(strTableName);

        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(tableName);

            List list = new ArrayList();
            for (String rowkey : strRowkeys) {
                Delete d = new Delete(rowkey.getBytes());
                list.add(d);
            }
            table.delete(list);
            LOG.info("=====删除行 " + strRowkeys + " 完成=====");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteColumnFamilies(String strTableName, String[] strCFNames) {

        if (strCFNames == null) {
            LOG.info("列族名为空,未删除列族");
            return;
        }

        TableName tableName = TableName.valueOf(strTableName);

        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin hBaseAdmin = connection.getAdmin();
            Table table = connection.getTable(tableName);

            hBaseAdmin.disableTable(tableName);

            HTableDescriptor tableDescriptor = hBaseAdmin.getTableDescriptor(tableName);

            for (String s : strCFNames) {
                try {
                    //TO-DO:如果表中无此列族名，直接结束方法
                    tableDescriptor.removeFamily(s.getBytes());
                } catch (Exception e) {
                    e.printStackTrace();
                    LOG.info("列族删除失败");
                }
            }

            hBaseAdmin.modifyTable(tableName, tableDescriptor);
            hBaseAdmin.enableTable(tableName);

            hBaseAdmin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // 创建新表、新列族

    /**
     * 建表方法
     *
     * @param strTableName 表名
     * @param strCFNames   列族名，多个请用逗号分隔
     * @param minV         列族保存的最小版本
     * @param maxV         列族保存的最大版本
     */
    public void createTable(String strTableName, String strCFNames, int minV, int maxV) {

        TableName tableName = TableName.valueOf(strTableName);
        List<String> columnFamilyNames = StringHelper.strToList(strCFNames);

        if (strCFNames.trim() == "") {
            LOG.info("建表时请指定至少一个列族名。");
            return;
        }

        LOG.info("=====开始建表" + strTableName + "=====");
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin hBaseAdmin = connection.getAdmin();

            // 如果存在要创建的表，那么先删除，再创建
            if (hBaseAdmin.tableExists(tableName)) {
                hBaseAdmin.disableTable(tableName);
                LOG.info(strTableName + "表已存在，正在删除");
                hBaseAdmin.deleteTable(tableName);
            }

            //使用新的命名空间，默认为 NamespaceDescriptor.DEFAULT_NAMESPACE
//            hBaseAdmin.createNamespace(NamespaceDescriptor.create("newns").build());

            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

            for (String cf : columnFamilyNames) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                //设置数据保存的版本个数
//                columnDescriptor.setVersions(minV, maxV);
                columnDescriptor.setMinVersions(minV);
                columnDescriptor.setMaxVersions(maxV);
                tableDescriptor.addFamily(columnDescriptor);
            }

            //异步写WAL日志，提高容错率
//            tableDescriptor.setDurability(Durability.ASYNC_WAL);

            hBaseAdmin.createTable(tableDescriptor);
            hBaseAdmin.close();
            connection.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("=====" + strTableName + "表创建完成");
    }

    /**
     * 建表时每个列族默认存2个版本
     */
    public void createTable(String strTableName, String strCFNames) {
        this.createTable(strTableName, strCFNames, 2, 2);
    }

    public void addNewColumnFamilies(String strTableName, String[] strCFNames) {

        if (strCFNames == null) {
            LOG.info("列族名不能为空");
            return;
        }

        TableName tableName = TableName.valueOf(strTableName);

        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin hBaseAdmin = connection.getAdmin();
            Table table = connection.getTable(tableName);

            hBaseAdmin.disableTable(tableName);

            HTableDescriptor tableDescriptor = hBaseAdmin.getTableDescriptor(tableName);

            for (String s : strCFNames) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(s);
                tableDescriptor.addFamily(columnDescriptor);
            }

            hBaseAdmin.modifyTable(tableName, tableDescriptor);
            hBaseAdmin.enableTable(tableName);

            hBaseAdmin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // 表的基本信息查询操作

    /**
     * 查询表的列族名
     */
    public void showColumnFamilies(String strTableName) {

        //返回的列族名数组
        List<String> resultCFs = new ArrayList<String>();

        TableName tableName = TableName.valueOf(strTableName);

        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(strTableName));

            HTableDescriptor tableDescriptor = table.getTableDescriptor();

            HColumnDescriptor[] columnDescriptors = tableDescriptor.getColumnFamilies();

            for (HColumnDescriptor columnDescriptor : columnDescriptors) {
                resultCFs.add(columnDescriptor.getNameAsString());
                //LOG.info("=====" + strTableName + "的列族有：" + columnDescriptor.getNameAsString());
            }

            LOG.info("=====" + strTableName + "的列族总共有：" + columnDescriptors.length + " 个");
            LOG.info("=====" + strTableName + "的列族分别是：" + resultCFs);

            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
