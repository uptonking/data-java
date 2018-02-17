package player.data.kylin;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.WeekFields;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by yaoo on 2/17/18
 */
public class BDPDate1416Mocker {

    /**
     * 收集起始时间到结束时间之间所有的时间并以字符串集合方式返回
     *
     * @param timeStart
     * @param timeEnd
     * @return
     */
    public static List<String> collectLocalDates(String timeStart, String timeEnd) {
        return collectLocalDates(LocalDate.parse(timeStart), LocalDate.parse(timeEnd));
    }

    /**
     * 收集起始时间到结束时间之间所有的时间并以字符串集合方式返回
     *
     * @param start
     * @param end
     * @return
     */
    public static List<String> collectLocalDates(LocalDate start, LocalDate end) {
        // 用起始时间作为流的源头，按照每次加一天的方式创建一个无限流
        return Stream.iterate(start, localDate -> localDate.plusDays(1))
                // 截断无限流，长度为起始时间和结束时间的差+1个
                .limit(ChronoUnit.DAYS.between(start, end) + 1)
                // 由于最后要的是字符串，所以map转换一下
                .map(LocalDate::toString)
                // 把流收集为List
                .collect(Collectors.toList());
    }


    public static List<Map> mockDateDimensionTableList(LocalDate start, LocalDate end) {


        return Stream.iterate(start, localDate -> localDate.plusDays(1))
                // 截断无限流，长度为起始时间和结束时间的差+1个
                .limit(ChronoUnit.DAYS.between(start, end) + 1)
                // 由于最后要的是字符串，所以map转换一下
                .map(d -> {
                    Map rowMap = new LinkedHashMap(1000);

                    String calDt = d.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    String yearBegDt = calDt.substring(0, 5) + "01-01";
                    String monBegDt = calDt.substring(0, 8) + "01";
                    String dayOfMonth = calDt.substring(8);
                    dayOfMonth = dayOfMonth.substring(0, 1).equals("0") ? dayOfMonth.substring(1) : dayOfMonth;

                    WeekFields weekFields = WeekFields.of(Locale.CHINESE);

                    String weekBegDt = d.with(weekFields.dayOfWeek(), 1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

                    String weekNumOfYear = String.valueOf(d.get(weekFields.weekOfWeekBasedYear()));

                    String dayOfWeek = String.valueOf(d.getDayOfWeek().getValue());

                    rowMap.put("calDt", calDt);
                    rowMap.put("yearBegDt", yearBegDt);
                    rowMap.put("monBegDt", monBegDt);
                    rowMap.put("weekBegDt", weekBegDt);
                    rowMap.put("weekNumOfYear", weekNumOfYear);
                    rowMap.put("dayOfMonth", dayOfMonth);
                    rowMap.put("dayOfWeek", dayOfWeek);

                    return rowMap;
                })
                // 把流收集为List
                .collect(Collectors.toList());
    }


    public static void main(String[] args) {

        String dateStart = "2014-01-01";
        String dateEnd = "2016-03-21";

        List<Map> list = mockDateDimensionTableList(LocalDate.parse(dateStart), LocalDate.parse(dateEnd));
//        list.forEach(System.out::println);

        LinkedHashMap headerMap = new LinkedHashMap();
        headerMap.put("calDt", "calDt");
        headerMap.put("yearBegDt", "yearBegDt");
        headerMap.put("monBegDt", "monBegDt");
        headerMap.put("weekBegDt", "weekBegDt");
        headerMap.put("weekNumOfYear", "weekNumOfYear");
        headerMap.put("dayOfMonth", "dayOfMonth");
        headerMap.put("dayOfWeek", "dayOfWeek");

        String path = "/root/Downloads/";
        String fileName = "bdp_date_1416";
        CSVUtil.createCSVFile(list, headerMap, path, fileName, false);


    }

}
