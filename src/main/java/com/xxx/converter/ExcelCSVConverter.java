package com.xxx.converter;

import com.xxx.io.CSVWriter;
import com.xxx.io.Excel2003Reader;
import com.xxx.io.Excel2007Reader;

import java.io.FileNotFoundException;

/**
 * Created by yaoo on 3/12/18
 */
public class ExcelCSVConverter {

    private CSVWriter csvWriter;
    private Excel2003Reader xlsReader;
    private Excel2007Reader xlsxReader;


    public static boolean xlsx2csv(String xlsxPath,String csvPath) throws FileNotFoundException {

//        xlsxReader = new Excel2007Reader(xlsxPath);
//        csvWriter = new CSVWriter(csvPath);

        return true;
    }

}
