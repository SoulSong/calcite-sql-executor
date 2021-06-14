package com.shf.calcite.csv;

import com.shf.calcite.executor.ExecutorTemplate;
import org.junit.Test;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/4 1:49
 */
public class CsvTest {

    @Test
    public void querySql() {
        String[] strArray = {
                "select * from CSV_1.MUSIC ",
                "SELECT album,fYear FROM ( select artist,album,fYear from CSV_1.MUSIC ) WHERE artist='lin junjie'",
                "select artist,count(*) as albumNum from CSV_1.MUSIC group by artist",
                "select t1.*,t2.* from CSV_1.MUSIC as t1 left join CSV_2.ARTIST as t2 on t1.artist=t2.artist"
        };

        ExecutorTemplate executorTemplate = new ExecutorTemplate("/csv.json", true);
        for (String sql : strArray) {
            executorTemplate.query(sql);
        }
    }
}
