package com.xxx.io;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public interface ImportReader {
    /**
     * 处理导入数据
     */
    public void process() throws Exception;

    /**
     * 设置读取行方式
     */
    public void setRowReader(RowReader rowReader);

    /**
     * 获取列名
     */
    public Map<String, String> getHeader();

    /**
     * 获取导入总行数
     */
    public int getRowCount();

    /**
     * 关闭
     */
    public void close() throws IOException, SQLException;
}
