package com.shf.calcite.dynamodb.mapper;

import com.shf.calcite.dynamodb.AbstractDynamoDbTest;
import com.shf.calcite.executor.ExecutorTemplate;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/12 2:25
 */
@Slf4j
public class DynamoDbMapperTest extends AbstractDynamoDbTest {
    @Test
    public void sqlQuery() throws Exception {
        String[] strArray = {
                "select * from dynamodb_mapper.t_Music t1 where artist='lin junjie'",
                "select fyear,album,T1.title from dynamodb_mapper.t_Music t1 where artist='lin junjie' and fyear=2006 and title='caocao'",
                "select t1.*,t2.* from dynamodb_mapper.t_Music t1 inner join dynamodb_mapper.t_Music t2 on t1.fyear = t2.fyear where t1.artist='lin junjie' and t2.artist = 'zhou jielun'",
        };

        ExecutorTemplate executorTemplate = new ExecutorTemplate("/mapper.json", true);
        for (String sql : strArray) {
            executorTemplate.query(sql);
        }
    }

}
