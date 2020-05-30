/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.consumer;

import com.linkedin.kmf.services.configs.CommonServiceConfig;
import com.linkedin.kmf.services.configs.ConsumeServiceConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NewConsumerKMBaseConsumerFactoryImpl implements KMBaseConsumerFactory {
  private final String _topic;
  private static final String FALSE = "false";
  private static final String[] NON_OVERRIDABLE_PROPERTIES =
      new String[]{ConsumeServiceConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumeServiceConfig.ZOOKEEPER_CONNECT_CONFIG};
  private static AdminClient adminClient;
  private static final Logger LOG = LoggerFactory.getLogger(NewConsumerKMBaseConsumerFactoryImpl.class);
  private final Properties _consumerProps;

  public NewConsumerKMBaseConsumerFactoryImpl(Map<String, Object> props) {
    LOG.info("Creating AdminClient.");
    adminClient = AdminClient.create(props);
    Map consumerPropsOverride = props.containsKey(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG) ? (Map) props.get(
        ConsumeServiceConfig.CONSUMER_PROPS_CONFIG) : new HashMap<>();
    ConsumeServiceConfig config = new ConsumeServiceConfig(props);
    _topic = config.getString(ConsumeServiceConfig.TOPIC_CONFIG);
    String zkConnect = config.getString(ConsumeServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
    String brokerList = config.getString(ConsumeServiceConfig.BOOTSTRAP_SERVERS_CONFIG);
    for (String property : NON_OVERRIDABLE_PROPERTIES) {
      if (consumerPropsOverride.containsKey(property)) {
        throw new ConfigException("Override must not contain " + property + " config.");
      }
    }
    _consumerProps = new Properties();

    /* Assign default config. This has the lowest priority. */
    _consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, FALSE);
    _consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    _consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "kmf-consumer");
    _consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kmf-consumer-group-" + new Random().nextInt());
    _consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    _consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    /* Assign config specified for ConsumeService. */
    _consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    _consumerProps.put(CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect);

    /* Assign config specified for consumer. This has the highest priority. */
    _consumerProps.putAll(consumerPropsOverride);

    if (props.containsKey(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG)) {
      props.forEach(_consumerProps::putIfAbsent);
    }
  }

  @Override
  public KMBaseConsumer create() throws Exception {

    return new NewConsumer(_topic, _consumerProps, adminClient);
  }
}
