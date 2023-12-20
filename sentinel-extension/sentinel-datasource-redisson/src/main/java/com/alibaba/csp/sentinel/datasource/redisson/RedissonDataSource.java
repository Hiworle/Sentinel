/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.csp.sentinel.datasource.redisson;

import com.alibaba.csp.sentinel.datasource.AbstractDataSource;
import com.alibaba.csp.sentinel.datasource.Converter;
import com.alibaba.csp.sentinel.datasource.redisson.config.RedissonConnectionConfig;
import com.alibaba.csp.sentinel.log.RecordLog;
import com.alibaba.csp.sentinel.util.AssertUtil;
import com.alibaba.csp.sentinel.util.StringUtil;
import io.lettuce.core.RedisClient;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.MessageListener;
import org.redisson.config.Config;

/**
 * <p>
 * A read-only {@code DataSource} with Redis backend.
 * </p>
 * <p>
 * The data source first loads initial rules from a Redis String during initialization.
 * Then the data source subscribe from specific channel. When new rules is published to the channel,
 * the data source will observe the change in realtime and update to memory.
 * </p>
 * <p>
 * Note that for consistency, users should publish the value and save the value to the ruleKey simultaneously
 * like this (using Redis transaction):
 * <pre>
 *  MULTI
 *  SET ruleKey value
 *  PUBLISH channel value
 *  EXEC
 * </pre>
 * </p>
 *
 * @author tiger, hope
 */
public class RedissonDataSource<T> extends AbstractDataSource<String, T> {

    private final RedissonClient redissonClient;

    private final String ruleKey;

    /**
     * Constructor of {@code RedisDataSource}.
     *
     * @param connectionConfig Redis connection config
     * @param ruleKey          data key in Redis
     * @param channel          channel to subscribe in Redis
     * @param parser           customized data parser, cannot be empty
     */
    public RedissonDataSource(RedissonConnectionConfig connectionConfig, String ruleKey, String channel,
                              Converter<String, T> parser) {
        super(parser);
        AssertUtil.notNull(connectionConfig, "Redis connection config can not be null");
        AssertUtil.notEmpty(ruleKey, "Redis ruleKey can not be empty");
        AssertUtil.notEmpty(channel, "Redis subscribe channel can not be empty");
        if (connectionConfig.getRedisClusters().isEmpty()) {
            this.redissonClient = getRedisClient(connectionConfig);
        } else {
            this.redissonClient = getRedisClusterClient(connectionConfig);
        }
        this.ruleKey = ruleKey;
        loadInitialConfig();
        subscribeFromChannel(channel);
    }

    /**
     * Build Redis client fromm {@code RedisConnectionConfig}.
     *
     * @return a new {@link RedisClient}
     */
    private RedissonClient getRedisClient(RedissonConnectionConfig connectionConfig) {
        if (connectionConfig.getRedisSentinels().isEmpty()) {
            RecordLog.info("[RedisDataSource] Creating stand-alone mode Redis client");
            return getRedisStandaloneClient(connectionConfig);
        } else {
            RecordLog.info("[RedisDataSource] Creating Redis Sentinel mode Redis client");
            return getRedisSentinelClient(connectionConfig);
        }
    }

    private RedissonClient getRedisClusterClient(RedissonConnectionConfig connectionConfig) {
        String password = new String(connectionConfig.getPassword());
        String clientName = connectionConfig.getClientName();

        Config clusterConfig = new Config();
        for (RedissonConnectionConfig config : connectionConfig.getRedisClusters()) {
            clusterConfig.useClusterServers()
                    .addNodeAddress(config.getHost() + ':' + config.getPort())
                    .setTimeout(connectionConfig.getTimeout());
        }

        if (!password.isEmpty()) {
            clusterConfig.useClusterServers().setPassword(password);
        }

        if (StringUtil.isNotEmpty(clientName)) {
            clusterConfig.useClusterServers().setClientName(clientName);
        }
        return Redisson.create(clusterConfig);
    }

    private RedissonClient getRedisStandaloneClient(RedissonConnectionConfig connectionConfig) {
        String password = new String(connectionConfig.getPassword());
        String clientName = connectionConfig.getClientName();
        Config config = new Config();

        config.useSingleServer()
                .setAddress(connectionConfig.getHost() + ':' + connectionConfig.getPort())
                .setDatabase(connectionConfig.getDatabase())
                .setTimeout(connectionConfig.getTimeout());
        if (!password.isEmpty()) {
            config.useSingleServer().setPassword(password);
        }
        if (StringUtil.isNotEmpty(connectionConfig.getClientName())) {
            config.useSingleServer().setClientName(clientName);
        }

        return Redisson.create(config);
    }

    private RedissonClient getRedisSentinelClient(RedissonConnectionConfig connectionConfig) {
        String password = new String(connectionConfig.getPassword());
        String clientName = connectionConfig.getClientName();
        Config sentinelConfig = new Config();

        connectionConfig.getRedisSentinels().forEach(config -> {
            sentinelConfig.useSentinelServers().addSentinelAddress(config.getHost() + ':' + config.getPort());
        });

        if (!password.isEmpty()) {
            sentinelConfig.useSentinelServers().setPassword(password);
        }
        if (StringUtil.isNotEmpty(connectionConfig.getClientName())) {
            sentinelConfig.useSentinelServers().setClientName(clientName);
        }
        sentinelConfig.useMasterSlaveServers().setMasterAddress(connectionConfig.getRedisSentinelMasterAddress())
                .setTimeout(connectionConfig.getTimeout());
        return Redisson.create(sentinelConfig);
    }

    private void subscribeFromChannel(String channel) {
        DelegatingMsgListener msgListener = new DelegatingMsgListener();
        RTopic topic = redissonClient.getTopic(channel);
        topic.addListener(String.class, msgListener);
    }

    private void loadInitialConfig() {
        try {
            T newValue = loadConfig();
            if (newValue == null) {
                RecordLog.warn("[RedisDataSource] WARN: initial config is null, you may have to check your data source");
            }
            getProperty().updateValue(newValue);
        } catch (Exception ex) {
            RecordLog.warn("[RedisDataSource] Error when loading initial config", ex);
        }
    }

    @Override
    public String readSource() {
        if (this.redissonClient == null) {
            throw new IllegalStateException("Redis client or Redis Cluster client has not been initialized or error occurred");
        }
        RBucket<String> bucket = redissonClient.getBucket(ruleKey);
        return bucket.get();
    }

    @Override
    public void close() {
        if (redissonClient != null) {
            redissonClient.shutdown();
        }
    }

    private class DelegatingMsgListener implements MessageListener<String> {

        DelegatingMsgListener() {
        }

        @Override
        public void onMessage(CharSequence channel, String msg) {
            RecordLog.info("[RedisDataSource] New property value received for channel {}: {}", channel, msg);
            getProperty().updateValue(parser.convert(msg));
        }
    }
}
