package com.shf.calcite.dynamodb.dynamic;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.concurrent.TimeUnit;

/**
 * description :
 * 存储动态表对应的查询参数
 *
 * @author songhaifeng
 * @date 2021/6/5 0:43
 */
class StatementCache {

    static Cache<String, String> CACHE = Caffeine.newBuilder()
            .maximumSize(100)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();
}
