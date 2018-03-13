package player.data.format;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

/**
 * ExcelCSVConverter Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>Mar 13, 2018</pre>
 */
public class ExcelCSVConverterTest {


    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: csvToExcel(String csvFileName, String excelFileName)
     */
    @Test
    public void testCsvToExcel() throws Exception {

        String fCSV = "/root/Documents/dataseed/sampledata/知乎用户sample.csv";
        String fXlsx = "/root/Downloads/知乎用户sample.xlsx";
        String fCSVNew = "/root/Downloads/知乎用户sample2.csv";

//        ExcelCSVConverter.csvToExcel(fCSV, fXlsx);
//        ExcelCSVConverter.excelToCSV(fXlsx, fCSVNew);

    }

    /**
     * Method: excelToCSV(String excelFileName, String csvFileName)
     */
    @Test
    public void testExcelToCSV() throws Exception {
//TODO: Test goes here...
    }


    /**
     * Method: writeCSV(String csvFileName, String csvData)
     */
    @Test
    public void testWriteCSV() throws Exception {
//TODO: Test goes here...
/*
try {
   Method method = ExcelCSVConverter.getClass().getMethod("writeCSV", String.class, String.class);
   method.setAccessible(true);
   method.invoke(<Object>, <Parameters>);
} catch(NoSuchMethodException e) {
} catch(IllegalAccessException e) {
} catch(InvocationTargetException e) {
}
*/
    }

    /**
     * Method: getCellData(HSSFCell myCell)
     */
    @Test
    public void testGetCellData() throws Exception {
//TODO: Test goes here...
/*
try {
   Method method = ExcelCSVConverter.getClass().getMethod("getCellData", HSSFCell.class);
   method.setAccessible(true);
   method.invoke(<Object>, <Parameters>);
} catch(NoSuchMethodException e) {
} catch(IllegalAccessException e) {
} catch(InvocationTargetException e) {
}
*/
    }

    /**
     * Method: getFormulaValue(HSSFCell myCell)
     */
    @Test
    public void testGetFormulaValue() throws Exception {
//TODO: Test goes here...
/*
try {
   Method method = ExcelCSVConverter.getClass().getMethod("getFormulaValue", HSSFCell.class);
   method.setAccessible(true);
   method.invoke(<Object>, <Parameters>);
} catch(NoSuchMethodException e) {
} catch(IllegalAccessException e) {
} catch(InvocationTargetException e) {
}
*/
    }

    /**
     * Method: getNumericValue(HSSFCell myCell)
     */
    @Test
    public void testGetNumericValue() throws Exception {
//TODO: Test goes here...
/*
try {
   Method method = ExcelCSVConverter.getClass().getMethod("getNumericValue", HSSFCell.class);
   method.setAccessible(true);
   method.invoke(<Object>, <Parameters>);
} catch(NoSuchMethodException e) {
} catch(IllegalAccessException e) {
} catch(InvocationTargetException e) {
}
*/
    }

    /**
     * Method: checkValidFile(String fileName)
     */
    @Test
    public void testCheckValidFile() throws Exception {
//TODO: Test goes here...
/*
try {
   Method method = ExcelCSVConverter.getClass().getMethod("checkValidFile", String.class);
   method.setAccessible(true);
   method.invoke(<Object>, <Parameters>);
} catch(NoSuchMethodException e) {
} catch(IllegalAccessException e) {
} catch(InvocationTargetException e) {
}
*/
    }

}
