// Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
//
// This software, the RabbitMQ Stream Java client library, is dual-licensed under the
// Mozilla Public License 2.0 ("MPL"), and the Apache License version 2 ("ASL").
// For the MPL, please see LICENSE-MPL-RabbitMQ. For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.
package com.rabbitmq.stream;

import static com.rabbitmq.stream.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.stream.TestUtils.CallableConsumer;
import io.netty.channel.EventLoopGroup;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class SuperStreamExchangeTest {

  EventLoopGroup eventLoopGroup;

  Environment environment;

  Connection connection;
  int partitions = 3;
  int messageCount = 10_000;
  String superStream;

  @BeforeEach
  void init(TestInfo info) throws Exception {
    EnvironmentBuilder environmentBuilder =
        Environment.builder()
            .port(TestUtils.streamPortNode1())
            .netty()
            .eventLoopGroup(eventLoopGroup)
            .environmentBuilder();
    environment = environmentBuilder.build();
    ConnectionFactory cf = new ConnectionFactory();
    cf.setPort(TestUtils.amqpPortNode1());
    connection = cf.newConnection();
    superStream = TestUtils.streamName(info);
  }

  @AfterEach
  void tearDown() throws Exception {
    environment.close();
    deleteSuperStreamTopology(connection, superStream, partitions);
    connection.close();
  }

  @Test
  void publish() throws Exception {
    declareSuperStreamTopology(connection, superStream, partitions);
    List<String> routingKeys = new ArrayList<>(messageCount);
    IntStream.range(0, messageCount)
        .forEach(ignored -> routingKeys.add(UUID.randomUUID().toString()));

    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    try (Producer producer =
        environment
            .producerBuilder()
            .superStream(superStream)
            .routing(msg -> msg.getProperties().getMessageIdAsString())
            .producerBuilder()
            .build()) {
      ConfirmationHandler confirmationHandler = status -> publishLatch.countDown();
      routingKeys.forEach(
          rk ->
              producer.send(
                  producer.messageBuilder().properties().messageId(rk).messageBuilder().build(),
                  confirmationHandler));

      assertThat(publishLatch).is(completed());
    }

    CallableConsumer<Map<String, Set<String>>> consumeMessages =
        receivedMessages -> {
          CountDownLatch consumeLatch = new CountDownLatch(messageCount);
          try (Consumer ignored =
              environment
                  .consumerBuilder()
                  .superStream(superStream)
                  .offset(OffsetSpecification.first())
                  .messageHandler(
                      (ctx, msg) -> {
                        receivedMessages
                            .computeIfAbsent(ctx.stream(), k -> ConcurrentHashMap.newKeySet())
                            .add(msg.getProperties().getMessageIdAsString());
                        consumeLatch.countDown();
                      })
                  .build()) {

            assertThat(consumeLatch).is(completed());
            assertThat(receivedMessages.values().stream().mapToInt(Set::size).sum())
                .isEqualTo(messageCount);
          }
        };

    Map<String, Set<String>> streamProducerMessages = new ConcurrentHashMap<>(partitions);
    consumeMessages.accept(streamProducerMessages);

    deleteSuperStreamTopology(connection, superStream, partitions);
    declareSuperStreamTopology(connection, superStream, partitions);

    try (Channel channel = connection.createChannel()) {
      channel.confirmSelect();
      for (String rk : routingKeys) {
        channel.basicPublish(
            superStream, rk, new AMQP.BasicProperties.Builder().messageId(rk).build(), null);
      }
      channel.waitForConfirmsOrDie();
    }

    Map<String, Set<String>> amqpProducerMessages = new ConcurrentHashMap<>(partitions);
    consumeMessages.accept(amqpProducerMessages);
    assertThat(amqpProducerMessages)
        .hasSameSizeAs(streamProducerMessages)
        .containsKeys(streamProducerMessages.keySet().toArray(new String[] {}));

    BiConsumer<Set<String>, Set<String>> compareSets =
        (s1, s2) -> {
          assertThat(s1).hasSameSizeAs(s2);
          s1.forEach(rk -> assertThat(s2).contains(rk));
        };

    amqpProducerMessages.forEach(
        (key, value) -> compareSets.accept(value, streamProducerMessages.get(key)));
  }
}
