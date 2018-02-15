package player.data.hbase;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.util.Arrays;

/**
 * HBaseCRUDUtil Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>Feb 15, 2018</pre>
 */
public class HBaseCRUDUtilTest {

    private HBaseCRUDUtil hbaseUtil;

    @Before
    public void before() throws Exception {

        String[][] arrConf = {
                {"hbase.zookeeper.quorum", "127.0.0.1"},
                {"hbase.zookeeper.property.clientPort", "2181"},
                {"hbase.master", "127.0.0.1:16000"},
//                {"zookeeper.znode.parent", "/hbase-unsecure"}

        };

        hbaseUtil = new HBaseCRUDUtil(arrConf);

    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: listAllTables()
     */
    @Test
    public void testListAllTables() throws Exception {
        System.out.println(Arrays.toString(hbaseUtil.listAllTables()));
    }

    /**
     * Method: queryByRowkey(String strTableName, String strRowkey)
     */
    @Test
    public void testQueryByRowkey() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: queryByColumnValue(String strTableName, String strCF, String strQualifier, String strValue)
     */
    @Test
    public void testQueryByColumnValue() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: queryByMultiColumns(String strTableName, String[] strCFs, String[] strQualifers, String[] strValues)
     */
    @Test
    public void testQueryByMultiColumns() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: queryAll(String strTableName)
     */
    @Test
    public void testQueryAll() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: insertRow(String strTableName, String strRowkey, String strColumnFamily, List<String> strQualifier, List<String> strValue)
     */
    @Test
    public void testInsertRow() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: deleteTable(String strTableName)
     */
    @Test
    public void testDeleteTable() throws Exception {

//        System.out.println("====最初表有");
//        System.out.println(Arrays.toString(hbaseUtil.listAllTables()));
//        System.out.println();
//
//        hbaseUtil.deleteTable("kylin_metadata");
//        System.out.println("====删除后表有");
//        System.out.println(Arrays.toString(hbaseUtil.listAllTables()));

    }

    /**
     * Method: deleteRowsByRowkeys(String strTableName, String[] strRowkeys)
     */
    @Test
    public void testDeleteRowsByRowkeys() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: deleteColumnFamilies(String strTableName, String[] strCFNames)
     */
    @Test
    public void testDeleteColumnFamilies() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: createTable(String strTableName, String strCFNames, int minV, int maxV)
     */
    @Test
    public void testCreateTableForStrTableNameStrCFNamesMinVMaxV() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: createTable(String strTableName, String strCFNames)
     */
    @Test
    public void testCreateTableForStrTableNameStrCFNames() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: addNewColumnFamilies(String strTableName, String[] strCFNames)
     */
    @Test
    public void testAddNewColumnFamilies() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: showColumnFamilies(String strTableName)
     */
    @Test
    public void testShowColumnFamilies() throws Exception {
//TODO: Test goes here...
    }


}
