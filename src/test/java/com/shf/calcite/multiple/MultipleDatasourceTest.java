package com.shf.calcite.multiple;

import com.shf.calcite.executor.ExecutorTemplate;
import org.junit.Test;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/14 21:08
 */
public class MultipleDatasourceTest {

    @Test
    public void sqlQuery() throws Exception {
        String[] strArray = {
                "select t1.*,t2.* from dynamodb_mapper.t_Music t1 inner join CSV_2.ARTIST t2 on t1.artist = t2.artist where t1.artist='lin junjie'",
        };

        ExecutorTemplate executorTemplate = new ExecutorTemplate("/multiple.json", true);
        for (String sql : strArray) {
            executorTemplate.query(sql);
        }
    }


}
