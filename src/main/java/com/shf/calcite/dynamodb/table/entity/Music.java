package com.shf.calcite.dynamodb.table.entity;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

/**
 * 约束：
 * 实体中字段schema定义全部小写，便于与sql文对应匹配
 * 表名、索引连接符采用`_`
 *
 * @author songhaifeng
 */
@DynamoDBTable(tableName = "t_music")
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Music {
    private static final transient String FYEAR_ARTIST_INDEX = "fyear_artist_index";
    private String artist;
    private String title;
    private String album;
    private int fyear;

    @DynamoDBHashKey(attributeName = "artist")
    @DynamoDBIndexRangeKey(globalSecondaryIndexName = FYEAR_ARTIST_INDEX, attributeName = "artist")
    public String getArtist() {
        return artist;
    }

    public void setArtist(String artist) {
        this.artist = artist;
    }

    @DynamoDBRangeKey(attributeName = "title")
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @DynamoDBAttribute(attributeName = "album")
    public String getAlbum() {
        return album;
    }

    public void setAlbum(String album) {
        this.album = album;
    }

    @DynamoDBAttribute(attributeName = "fyear")
    @DynamoDBIndexHashKey(globalSecondaryIndexName = FYEAR_ARTIST_INDEX, attributeName = "fyear")
    public int getFyear() {
        return fyear;
    }

    public void setFyear(int fyear) {
        this.fyear = fyear;
    }
}