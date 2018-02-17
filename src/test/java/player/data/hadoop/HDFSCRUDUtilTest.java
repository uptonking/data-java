package player.data.hadoop;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

/**
 * HDFSCRUDUtil Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>Feb 13, 2018</pre>
 */
public class HDFSCRUDUtilTest {

    private HDFSCRUDUtil dfsUtil;

    @Before
    public void before() throws Exception {
        String pathBase = "/opt/zoo/env/hadoop-2.7.2/etc/hadoop/";

        String[] hadoopConfArr = {
                pathBase + "core-site.xml",
                pathBase + "hdfs-site.xml",
                pathBase + "mapred-site.xml",
                pathBase + "yarn-site.xml"
        };

        dfsUtil = new HDFSCRUDUtil(hadoopConfArr);

    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: list(String srcPath)
     */
    @Test
    public void testList() throws Exception {
        System.out.println("====打印hdfs根目录");
        dfsUtil.list("/");
    }

    /**
     * Method: addFile(String source, String dest)
     */
    @Test
    public void testAddFile() throws Exception {
        System.out.println("====初始目录列表");
        dfsUtil.list("/hellodir");
        System.out.println();

        dfsUtil.addFile("//tempx/sample/cn/china.json", "/hellodir");
        System.out.println("====上传文件后");
        dfsUtil.list("/hellodir");
        System.out.println();

        dfsUtil.deleteFile("/hellodir/china.json");
        System.out.println("====删除文件后");
        dfsUtil.list("/hellodir");
        System.out.println();

        dfsUtil.fsClose();

    }

    /**
     * Method: mkdir(String dir)
     */
    @Test
    public void testMkdir() throws Exception {

        System.out.println("====初始目录列表");
        dfsUtil.list("/");
        System.out.println();

        dfsUtil.mkdir("/hellodir-abc");
        System.out.println("====创建新目录ABC后");
        dfsUtil.list("/");
        System.out.println();

        dfsUtil.deleteFile("/hellodir-abc");
        System.out.println("====删除新目录ABC后");
        dfsUtil.list("/");
        System.out.println();

        dfsUtil.fsClose();
    }

    /**
     * 读取的文件会保存在本地根目录
     * Method: readFile(String file)
     */
    @Test
    public void testReadFile() throws Exception {
//        dfsUtil.readFile("/hellodir/a2.txt");
//        dfsUtil.fsClose();
    }


    /**
     * Method: ifExists(String source)
     */
    @Test
    public void testIfExists() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: copyFromLocal(String source, String dest)
     */
    @Test
    public void testCopyFromLocal() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: renameFile(String fromthis, String tothis)
     */
    @Test
    public void testRenameFile() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: deleteFile(String file)
     */
    @Test
    public void testDeleteFile() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: getModificationTime(String source)
     */
    @Test
    public void testGetModificationTime() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: getBlockLocations(String source)
     */
    @Test
    public void testGetBlockLocations() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: getHostnames()
     */
    @Test
    public void testGetHostnames() throws Exception {
//TODO: Test goes here...
    }


}
