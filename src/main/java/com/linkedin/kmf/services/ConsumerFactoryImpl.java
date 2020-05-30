/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.services;

import com.linkedin.kmf.consumer.KMBaseConsumer;
import com.linkedin.kmf.consumer.KMBaseConsumerFactory;
import com.linkedin.kmf.services.configs.ConsumeServiceConfig;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerFactoryImpl implements ConsumerFactory {
  private final KMBaseConsumer _baseConsumer;
  private final String _topic;
  private final int _latencyPercentileMaxMs;
  private final int _latencyPercentileGranularityMs;
  private final int _latencySlaMs;
  private static AdminClient adminClient;
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerFactoryImpl.class);

  public ConsumerFactoryImpl(Map<String, Object> props) throws Exception {
    LOGGER.info("{} constructor starting..", this.getClass().getName());
    ConsumeServiceConfig config = new ConsumeServiceConfig(props);

    adminClient = AdminClient.create(props);
    _topic = config.getString(ConsumeServiceConfig.TOPIC_CONFIG);
    _latencySlaMs = config.getInt(ConsumeServiceConfig.LATENCY_SLA_MS_CONFIG);
    _latencyPercentileMaxMs = config.getInt(ConsumeServiceConfig.LATENCY_PERCENTILE_MAX_MS_CONFIG);
    _latencyPercentileGranularityMs = config.getInt(ConsumeServiceConfig.LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG);

    String consumerClassName = config.getString(ConsumeServiceConfig.CONSUMER_CLASS_CONFIG);
    String consumerFactoryImpl = consumerClassName + "KMBaseConsumerFactoryImpl";
    KMBaseConsumerFactory kmBaseConsumerFactory = (KMBaseConsumerFactory) Class.forName(consumerFactoryImpl)
        .getConstructor(Map.class)
        .newInstance(props);

    _baseConsumer = kmBaseConsumerFactory.create();
  }

  @Override
  public AdminClient adminClient() {
    return adminClient;
  }

  @Override
  public int latencySlaMs() {
    return _latencySlaMs;
  }

  @Override
  public KMBaseConsumer baseConsumer() {
    return _baseConsumer;
  }

  @Override
  public String topic() {
    return _topic;
  }

  @Override
  public int latencyPercentileMaxMs() {
    return _latencyPercentileMaxMs;
  }

  @Override
  public int latencyPercentileGranularityMs() {
    return _latencyPercentileGranularityMs;
  }
}


