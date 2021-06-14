package com.shf.calcite.dynamodb;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.shf.calcite.dynamodb.table.entity.Music;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/13 22:30
 */
@Slf4j
public class AbstractDynamoDbTest {
    private DynamoDBMapper dynamoDbMapper = DynamoDbClientFactory.dynamoDbMapper();

    static {
        System.getProperties().put("accessKey", "access_key_id");
        System.getProperties().put("secretKey", "secret_key_id");
        System.getProperties().put("region", "us-east-1");
        System.getProperties().put("endpoint", "http://127.0.0.1:8765");
    }

    /**
     * 创建带有二级索引的样例表
     */
    @Test
    public void createTable() {
        DynamoDB dynamoDB = new DynamoDB(DynamoDbClientFactory.dynamoDbClient());
        try {
            String tableName = "t_music";
            List<KeySchemaElement> keySchemaElementList = Arrays.asList(new KeySchemaElement("artist", KeyType.HASH),
                    new KeySchemaElement("title", KeyType.RANGE));
            List<AttributeDefinition> attributeDefinitionList = Arrays.asList(new AttributeDefinition("artist", ScalarAttributeType.S),
                    new AttributeDefinition("title", ScalarAttributeType.S)
                    , new AttributeDefinition("fyear", ScalarAttributeType.N));

            GlobalSecondaryIndex fyearArtistIndex = new GlobalSecondaryIndex().withIndexName("fyear_artist_index")
                    .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L))
                    .withKeySchema(new KeySchemaElement().withAttributeName("fyear").withKeyType(KeyType.HASH),
                            new KeySchemaElement().withAttributeName("artist").withKeyType(KeyType.RANGE))
                    .withProjection(
                            new Projection().withProjectionType("INCLUDE").withNonKeyAttributes("title"));

            CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                    .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L))
                    .withAttributeDefinitions(attributeDefinitionList).withKeySchema(keySchemaElementList)
                    .withGlobalSecondaryIndexes(fyearArtistIndex);

            Table table = dynamoDB.createTable(createTableRequest);
            table.waitForActive();
            log.info("succeed to create table: {}.", tableName);
        } catch (Exception e) {
            log.error("Unable to create table: {}", e.getMessage());
        } finally {
            dynamoDB.shutdown();
        }
    }

    /**
     * 初始化待查询数据集
     */
    @Test
    public void loadData() {
        dynamoDbMapper.batchSave(Music.builder().artist("lin junjie").title("caocao").album("曹操").fyear(2006).build()
                , Music.builder().artist("lin junjie").title("jiangnan").album("江南").fyear(2004).build()
                , Music.builder().artist("lin junjie").title("xijie").fyear(2007).build()
                , Music.builder().artist("lin junjie").title("xindiqiu").album("新地球").fyear(2014).build()
                , Music.builder().artist("zhou jielun").title("jiangnan").album("范特西").fyear(2001).build()
                , Music.builder().artist("zhou jielun").title("badukongjian").album("八度空间").fyear(2002).build()
                , Music.builder().artist("zhou jielun").title("jingtanhao").album("惊叹号").fyear(2010).build()
                , Music.builder().artist("zhou jielun").title("wohenmang").album("我很忙").fyear(2007).build()
        );
    }

    /**
     * 模拟常见的集中查询方式，便于后续基于calcite的实现参考
     */
    @Test
    public void query() {
        // 提供完整的hashKey和rangKey，则直接通过load完成查询
        Music music = dynamoDbMapper.load(Music.class, "lin junjie", "jiangnan");
        assert music.getAlbum().equals("江南");

        // 仅提供hashKey，指定projection投影字段
        HashMap<String, AttributeValue> eav = new HashMap<>();
        eav.put(":artist", new AttributeValue().withS("lin junjie"));
        DynamoDBQueryExpression dynamoDBQueryExpression = new DynamoDBQueryExpression();
        dynamoDBQueryExpression.withKeyConditionExpression("artist = :artist")
                .withExpressionAttributeValues(eav).withProjectionExpression("artist,album,fyear");
        PaginatedQueryList<Music> paginatedQueryList = dynamoDbMapper.query(Music.class, dynamoDBQueryExpression);
        assert paginatedQueryList.size() == 4;

        // 仅提供hashKey，同时增加了非rangKey的过滤条件，并指定projection投影字段
        eav = new HashMap<>();
        eav.put(":artist", new AttributeValue().withS("lin junjie"));
        eav.put(":fyear", new AttributeValue().withN("2006"));
        dynamoDBQueryExpression = new DynamoDBQueryExpression();
        dynamoDBQueryExpression.withKeyConditionExpression("artist = :artist")
                .withFilterExpression("fyear = :fyear")
                .withExpressionAttributeValues(eav).withProjectionExpression("artist,album,fyear");
        paginatedQueryList = dynamoDbMapper.query(Music.class, dynamoDBQueryExpression);
        assert paginatedQueryList.size() == 1;

        // 查询二级索引，提供hashKey和rangKey，同时指定projection投影字段
        eav = new HashMap<>();
        eav.put(":fyear", new AttributeValue().withN("2007"));
        eav.put(":artist", new AttributeValue().withS("zhou jielun"));
        dynamoDBQueryExpression = new DynamoDBQueryExpression();
        dynamoDBQueryExpression.withKeyConditionExpression("fyear = :fyear and artist = :artist ")
                .withIndexName("fyear_artist_index")
                .withConsistentRead(false)
                .withExpressionAttributeValues(eav).withProjectionExpression("artist,title,fyear");
        paginatedQueryList = dynamoDbMapper.query(Music.class, dynamoDBQueryExpression);
        assert paginatedQueryList.size() == 1;

        // 查询二级索引，仅提供hashKey，同时指定projection投影字段
        eav = new HashMap<>();
        eav.put(":fyear", new AttributeValue().withN("2007"));
        dynamoDBQueryExpression = new DynamoDBQueryExpression();
        dynamoDBQueryExpression.withKeyConditionExpression("fyear = :fyear")
                .withIndexName("fyear_artist_index")
                .withConsistentRead(false)
                .withExpressionAttributeValues(eav).withProjectionExpression("artist,title,fyear");
        paginatedQueryList = dynamoDbMapper.query(Music.class, dynamoDBQueryExpression);
        assert paginatedQueryList.size() == 2;
    }

    @Test
    public void dropTable() {
        DynamoDB dynamoDB = new DynamoDB(DynamoDbClientFactory.dynamoDbClient());
        try {
            String tableName = "t_music";
            dynamoDB.getTable(tableName).delete();
            log.info("succeed to delete table: {}.", tableName);
        } catch (Exception e) {
            log.error("Unable to delete table: {}", e.getMessage());
        } finally {
            dynamoDB.shutdown();
        }
    }

}
