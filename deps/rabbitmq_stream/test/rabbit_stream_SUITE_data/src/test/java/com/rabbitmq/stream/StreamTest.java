// The contents of this file are subject to the Mozilla Public License
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License
// at https://www.mozilla.org/en-US/MPL/2.0/
//
// Software distributed under the License is distributed on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
// the License for the specific language governing rights and
// limitations under the License.
//
// The Original Code is RabbitMQ.
//
// The Initial Developer of the Original Code is Pivotal Software, Inc.
// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
//

package com.rabbitmq.stream;

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.impl.Client;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class StreamTest {

  String stream;
  TestUtils.ClientFactory cf;

  static Stream<Arguments> shouldBePossibleToPublishFromAnyNodeAndConsumeFromAnyMember() {
    return Stream.of(
        brokers(
            "leader", metadata -> metadata.getLeader(), "leader", metadata -> metadata.getLeader()),
        brokers(
            "leader",
            metadata -> metadata.getLeader(),
            "replica",
            metadata -> metadata.getReplicas().iterator().next()),
        brokers(
            "replica",
            metadata -> metadata.getReplicas().iterator().next(),
            "leader",
            metadata -> metadata.getLeader()),
        brokers(
            "replica",
            metadata -> new ArrayList<>(metadata.getReplicas()).get(0),
            "replica",
            metadata -> new ArrayList<>(metadata.getReplicas()).get(1)));
  }

  static Arguments brokers(
      String dp,
      Function<Client.StreamMetadata, Client.Broker> publisherBroker,
      String dc,
      Function<Client.StreamMetadata, Client.Broker> consumerBroker) {
    return Arguments.of(
        new FunctionWithToString<>(dp, publisherBroker),
        new FunctionWithToString<>(dc, consumerBroker));
  }

  @ParameterizedTest
  @MethodSource
  void shouldBePossibleToPublishFromAnyNodeAndConsumeFromAnyMember(
      Function<Client.StreamMetadata, Client.Broker> publisherBroker,
      Function<Client.StreamMetadata, Client.Broker> consumerBroker)
      throws Exception {

    int messageCount = 10_000;
    Client client = cf.get(new Client.ClientParameters().port(TestUtils.streamPortNode1()));
    Map<String, Client.StreamMetadata> metadata = client.metadata(stream);
    assertThat(metadata).hasSize(1).containsKey(stream);

    TestUtils.waitUntil(() -> client.metadata(stream).get(stream).getReplicas().size() == 2);

    Client.StreamMetadata streamMetadata = client.metadata(stream).get(stream);

    CountDownLatch publishingLatch = new CountDownLatch(messageCount);
    Client publisher =
        cf.get(
            new Client.ClientParameters()
                .port(publisherBroker.apply(streamMetadata).getPort())
                .publishConfirmListener(
                    (publisherId, publishingId) -> publishingLatch.countDown()));

    publisher.declarePublisher((byte) 1, null, stream);
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                publisher.publish(
                    (byte) 1,
                    Collections.singletonList(
                        publisher
                            .messageBuilder()
                            .addData(("hello " + i).getBytes(StandardCharsets.UTF_8))
                            .build())));

    assertThat(publishingLatch.await(10, TimeUnit.SECONDS)).isTrue();

    CountDownLatch consumingLatch = new CountDownLatch(messageCount);
    Set<String> bodies = ConcurrentHashMap.newKeySet(messageCount);
    Client consumer =
        cf.get(
            new Client.ClientParameters()
                .port(consumerBroker.apply(streamMetadata).getPort())
                .chunkListener(
                    (client1, subscriptionId, offset, messageCount1, dataSize) ->
                        client1.credit(subscriptionId, 10))
                .messageListener(
                    (subscriptionId, offset, chunkTimestamp, message) -> {
                      bodies.add(new String(message.getBodyAsBinary(), StandardCharsets.UTF_8));
                      consumingLatch.countDown();
                    }));

    consumer.subscribe((byte) 1, stream, OffsetSpecification.first(), 10);

    assertThat(consumingLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(bodies).hasSize(messageCount);
    IntStream.range(0, messageCount).forEach(i -> assertThat(bodies.contains("hello " + i)));
  }

  @Test
  void metadataOnClusterShouldReturnLeaderAndReplicas() throws InterruptedException {
    Client client = cf.get(new Client.ClientParameters().port(TestUtils.streamPortNode1()));
    Map<String, Client.StreamMetadata> metadata = client.metadata(stream);
    assertThat(metadata).hasSize(1).containsKey(stream);
    assertThat(metadata.get(stream).getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

    TestUtils.waitUntil(() -> client.metadata(stream).get(stream).getReplicas().size() == 2);

    BiConsumer<Client.Broker, Client.Broker> assertNodesAreDifferent =
        (node, anotherNode) -> {
          assertThat(node.getHost()).isEqualTo(anotherNode.getHost());
          assertThat(node.getPort()).isNotEqualTo(anotherNode.getPort());
        };

    Client.StreamMetadata streamMetadata = client.metadata(stream).get(stream);

    streamMetadata
        .getReplicas()
        .forEach(replica -> assertNodesAreDifferent.accept(replica, streamMetadata.getLeader()));
    List<Client.Broker> replicas = new ArrayList<>(streamMetadata.getReplicas());
    assertNodesAreDifferent.accept(replicas.get(0), replicas.get(1));
  }

  static class FunctionWithToString<T, R> implements Function<T, R> {

    final String toString;
    final Function<T, R> delegate;

    FunctionWithToString(String toString, Function<T, R> delegate) {
      this.toString = toString;
      this.delegate = delegate;
    }

    @Override
    public R apply(T t) {
      return delegate.apply(t);
    }

    @Override
    public String toString() {
      return toString;
    }
  }
}
