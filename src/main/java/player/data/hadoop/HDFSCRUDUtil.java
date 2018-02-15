package player.data.hadoop;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS CRUD工具类
 * <p>
 * HDFS不支持多写，不支持在文件的任意位置修改
 */
@SuppressWarnings("unused")
public class HDFSCRUDUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSCRUDUtil.class);

    private Configuration conf;
    private FileSystem fs;

    public HDFSCRUDUtil() {
        this.conf = new Configuration();
        try {
            this.fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HDFSCRUDUtil(String[] arrConfPath) {
        this.conf = new Configuration();
        for (int i = 0; i < arrConfPath.length; i++) {
            conf.addResource(new Path(arrConfPath[i]));
        }
        try {
            this.fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 打印指定路径下该层的目录和文件
     * 没有递归遍历子目录
     * Reads the directory name(s) and file name(s)from the specified parameter "srcPath"
     *
     * @param srcPath
     */
    public void list(String srcPath) {
        LOG.info("[Defaultfs] - " + conf.get("fs.defaultFS"));

        try {
            RemoteIterator<LocatedFileStatus> rmIterator = fs.listLocatedStatus(new Path(srcPath));
            while (rmIterator.hasNext()) {
                Path path = rmIterator.next().getPath();
                if (fs.isDirectory(path)) {
                    LOG.info("-----------dir : " + path.getName());
                } else if (fs.isFile(path)) {
                    LOG.info("-----------file: " + path.getName());
                }
            }
        } catch (IOException e) {
            LOG.error("list fileSysetm object stream.:", e);
            new RuntimeException(e);
        }
    }


    /**
     * 上传文件到集群指定位置
     * <p>
     * Uploads or adds a file to HDFS
     *
     * @param source
     * @param dest
     */
    public void addFile(String source, String dest) {

        try {
            // Get the filename out of thefile path
            String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

            // Create the destination path including the filename.
            if (dest.charAt(dest.length() - 1) != '/') {
                dest = dest + "/" + filename;
            } else {
                dest = dest + filename;
            }

            // Check if the file alreadyexists
            Path path = new Path(dest);
            if (fs.exists(path)) {
                LOG.error("File " + dest + " already exists");
                return;
            }

            // Create a new file and writedata to it.
            FSDataOutputStream out = fs.create(path);
            InputStream in = new BufferedInputStream(new FileInputStream(
                    new File(source)));

            byte[] b = new byte[1024];
            int numBytes = 0;
            //In this way read and write datato destination file.
            while ((numBytes = in.read(b)) > 0) {
                out.write(b, 0, numBytes);
            }
            in.close();
            out.close();
//            fs.close();
        } catch (IOException e) {
            LOG.error("addFile Exception caught! :", e);
            new RuntimeException(e);
        }
    }

    /**
     * 创建目录
     * Makes the specified directory if it doesn'texist.
     *
     * @param dir
     */
    public void mkdir(String dir) {

        try {
//            fs = FileSystem.get(conf);
            Path path = new Path(dir);
            if (!fs.exists(path)) {
                fs.mkdirs(path);
                LOG.debug("create directory '" + dir + "' successfully!");
            } else {
                LOG.debug("directory '" + dir + "' exits!");
            }
        } catch (IOException e) {
            LOG.error("FileSystem get configuration with anerror");
            e.printStackTrace();
        }
//        finally {
//            if (fs != null) {
//                try {
//                    fs.close();
//                } catch (IOException e) {
//                    LOG.error("close fs object stream. :", e);
//                    new RuntimeException(e);
//                }
//            }
//        }
    }

    /**
     * 读取指定文件的内容
     * Reads the file content in console.
     *
     * @param file
     */
    public void readFile(String file) {

        try {
            Path path = new Path(file);
            if (!fs.exists(path)) {
                LOG.warn("file'" + file + "' doesn't exist!");
                return;
            }

            FSDataInputStream in = fs.open(path);
            String filename = file.substring(file.lastIndexOf('/') + 1, file.length());
            OutputStream out = new BufferedOutputStream(new FileOutputStream(
                    new File(filename)));

            byte[] b = new byte[1024];
            int numBytes = 0;
            while ((numBytes = in.read(b)) > 0) {
                out.write(b, 0, numBytes);
            }
            in.close();
            out.close();
//            fs.close();
        } catch (IOException e) {
            LOG.error("ifExists fs Exception caught! :", e);
            new RuntimeException(e);
        }
    }


    /**
     * 判断指定文件是否存在
     *
     * @param source
     * @return
     */
    public boolean ifExists(String source) {

        if (source == null || source.length() == 0) {
            return false;
        }

        try {
//            fs = FileSystem.get(conf);
            LOG.debug("judge file '" + source + "'");
            return fs.exists(new Path(source));
        } catch (IOException e) {
            LOG.error("ifExists fs Exception caught! :", e);
            new RuntimeException(e);
            return false;
        }
//        finally {
//            if (fs != null) {
//                try {
//                    fs.close();
//                } catch (IOException e) {
//                    LOG.error("fs.close Exception caught! :", e);
//                    new RuntimeException(e);
//                }
//            }
//
//        }

    }

    /**
     * 递归复制本地目录/文件到集群
     * Recursively copies the source path directories or files to the destination path of DFS.
     * It is the same functionality as thefollowing comand:
     * hadoopfs -copyFromLocal <local fs><hadoop fs>
     *
     * @param source
     * @param dest
     */
    public void copyFromLocal(String source, String dest) {

        try {
//            fs = FileSystem.get(conf);
            Path srcPath = new Path(source);

            Path dstPath = new Path(dest);
            // Check if the file alreadyexists
            if (!(fs.exists(dstPath))) {
                LOG.warn("dstPathpath doesn't exist");
                LOG.error("No such destination " + dstPath);
                return;
            }

            // Get the filename out of thefile path
            String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

            try {
                //if the file exists in thedestination path, it will throw exception.
//                                   fs.copyFromLocalFile(srcPath,dstPath);
                //remove and overwrite files withthe method
                //copyFromLocalFile(booleandelSrc, boolean overwrite, Path src, Path dst)
                fs.copyFromLocalFile(false, true, srcPath, dstPath);
                LOG.info("File " + filename + "copied to " + dest);
            } catch (Exception e) {
                LOG.error("copyFromLocalFile exception caught!:", e);
                new RuntimeException(e);
            }
//            finally {
//                fs.close();
//            }
        } catch (IOException e1) {
            LOG.error("copyFromLocal IOException objectstream. :", e1);
            new RuntimeException(e1);
        }
    }


    /**
     * 重命名
     *
     * @param fromthis
     * @param tothis
     */
    public void renameFile(String fromthis, String tothis) {

        try {
//            fs = FileSystem.get(conf);
            Path fromPath = new Path(fromthis);
            Path toPath = new Path(tothis);

            if (!(fs.exists(fromPath))) {
                LOG.info("No such destination " + fromPath);
                return;
            }

            if (fs.exists(toPath)) {
                LOG.info("Already exists! " + toPath);
                return;
            }

            try {
                boolean isRenamed = fs.rename(fromPath, toPath);     //renames file name indeed.
                if (isRenamed) {
                    LOG.info("Renamed from " + fromthis + " to " + tothis);
                }
            } catch (Exception e) {
                LOG.error("renameFile Exception caught! :", e);
                new RuntimeException(e);
            }
//            finally {
//                fs.close();
//            }
        } catch (IOException e1) {
            LOG.error("fs Exception caught! :", e1);
            new RuntimeException(e1);
        }
    }


    /**
     * 删除文件/目录
     * Deletes the files if it is a directory.
     *
     * @param file
     */
    public void deleteFile(String file) {

        try {
//            fs = FileSystem.get(conf);

            Path path = new Path(file);
            if (!fs.exists(path)) {
                LOG.info("File " + file + " does not exists");
                return;
            }
            /*
             * recursively delete the file(s) if it is adirectory.
             * If you want to mark the path that will bedeleted as
             * a result of closing the FileSystem.
             *  deleteOnExit(Path f)
             */
            fs.delete(new Path(file), true);
//            fs.close();
        } catch (IOException e) {
            LOG.error("deleteFile Exception caught! :", e);
            new RuntimeException(e);
        }

    }

    /**
     * 获取文件的修改时间
     * Gets the information about the file modifiedtime.
     *
     * @param source
     * @throws IOException
     */
    public void getModificationTime(String source) throws IOException {

        Path srcPath = new Path(source);

        // Check if the file alreadyexists
        if (!(fs.exists(srcPath))) {
            System.out.println("No such destination " + srcPath);
            return;
        }
        // Get the filename out of thefile path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        FileStatus fileStatus = fs.getFileStatus(srcPath);
        long modificationTime = fileStatus.getModificationTime();

        LOG.info("modified datetime: " + System.out.format("File %s; Modification time : %0.2f%n", filename, modificationTime));

    }

    /**
     * 获取文件所在数据块block信息
     * Gets the file block location info
     *
     * @param source
     * @throws IOException
     */
    public void getBlockLocations(String source) throws IOException {

        Path srcPath = new Path(source);

        // Check if the file alreadyexists
        if (!(ifExists(source))) {
            System.out.println("No such destination " + srcPath);
            return;
        }
        // Get the filename out of thefile path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        FileStatus fileStatus = fs.getFileStatus(srcPath);

        BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        int blkCount = blkLocations.length;

        System.out.println("File :" + filename + "stored at:");
        for (int i = 0; i < blkCount; i++) {
            String[] hosts = blkLocations[i].getHosts();
            LOG.info("host ip:" + System.out.format("Host %d: %s %n", i, hosts));
        }
    }

    /**
     * 获取datanode主机名
     *
     * @throws IOException
     */
    public void getHostnames() throws IOException {
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

        String[] names = new String[dataNodeStats.length];
        for (int i = 0; i < dataNodeStats.length; i++) {
            names[i] = dataNodeStats[i].getHostName();
            LOG.info("datenode hostname:" + (dataNodeStats[i].getHostName()));
        }
    }

    public void fsClose() {
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
