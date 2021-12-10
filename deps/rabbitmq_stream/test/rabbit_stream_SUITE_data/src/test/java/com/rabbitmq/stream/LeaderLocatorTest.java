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

import static com.rabbitmq.stream.TestUtils.ResponseConditions.ko;
import static com.rabbitmq.stream.TestUtils.ResponseConditions.ok;
import static com.rabbitmq.stream.TestUtils.ResponseConditions.responseCode;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.impl.Client;
import com.rabbitmq.stream.impl.Client.Broker;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.Client.StreamMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class LeaderLocatorTest {

  TestUtils.ClientFactory cf;

  @Test
  void invalidLocatorShouldReturnError() {
    Client client = cf.get(new Client.ClientParameters().port(TestUtils.streamPortNode1()));
    String s = UUID.randomUUID().toString();
    Response response = client.create(s, Collections.singletonMap("queue-leader-locator", "foo"));
    assertThat(response).is(ko()).has(responseCode(Constants.RESPONSE_CODE_PRECONDITION_FAILED));
  }

  @Test
  void clientLocalLocatorShouldMakeLeaderOnConnectedNode() {
    int[] ports = new int[] {TestUtils.streamPortNode1(), TestUtils.streamPortNode2()};
    for (int port : ports) {
      Client client = cf.get(new Client.ClientParameters().port(port));
      String s = UUID.randomUUID().toString();
      try {
        Response response =
            client.create(s, Collections.singletonMap("queue-leader-locator", "client-local"));
        assertThat(response).is(ok());
        StreamMetadata metadata = client.metadata(s).get(s);
        assertThat(metadata).isNotNull();
        assertThat(metadata.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
        assertThat(metadata.getLeader()).isNotNull().extracting(b -> b.getPort()).isEqualTo(port);
      } finally {
        client.delete(s);
      }
    }
  }

  @Test
  void randomLocatorShouldCreateOnAllNodesAfterSomeTime() throws Exception {
    int clusterSize = 3;
    Set<String> createdStreams = ConcurrentHashMap.newKeySet();
    Set<Broker> leaderNodes = ConcurrentHashMap.newKeySet(clusterSize);
    CountDownLatch latch = new CountDownLatch(1);
    Client client = cf.get(new ClientParameters().port(TestUtils.streamPortNode1()));
    Runnable runnable =
        () -> {
          while (leaderNodes.size() < clusterSize && !Thread.interrupted()) {
            String s = UUID.randomUUID().toString();
            Response response =
                client.create(s, Collections.singletonMap("queue-leader-locator", "random"));
            if (!response.isOk()) {
              break;
            }
            createdStreams.add(s);
            StreamMetadata metadata = client.metadata(s).get(s);
            if (metadata == null || !metadata.isResponseOk() || metadata.getLeader() == null) {
              break;
            }
            leaderNodes.add(metadata.getLeader());
          }
          latch.countDown();
        };

    Thread worker = new Thread(runnable);
    worker.start();

    try {
      assertThat(latch.await(10, SECONDS)).isTrue();
      assertThat(leaderNodes).hasSize(clusterSize);
      // in case Broker class is broken
      assertThat(leaderNodes.stream().map(b -> b.getPort()).collect(Collectors.toSet()))
          .hasSize(clusterSize);
    } finally {
      if (worker.isAlive()) {
        worker.interrupt();
      }
      createdStreams.forEach(
          s -> {
            Response response = client.delete(s);
            if (!response.isOk()) {
              LoggerFactory.getLogger(LeaderLocatorTest.class).warn("Error while deleting stream");
            }
          });
    }
  }

  @Test
  void leastLeadersShouldStreamLeadersOnTheCluster() {
    int clusterSize = 3;
    int streamsByNode = 5;
    int streamCount = clusterSize * streamsByNode;
    Set<String> createdStreams = ConcurrentHashMap.newKeySet();
    Client client = cf.get(new ClientParameters().port(TestUtils.streamPortNode1()));

    try {
      IntStream.range(0, streamCount)
          .forEach(
              i -> {
                String s = UUID.randomUUID().toString();
                Response response =
                    client.create(
                        s, Collections.singletonMap("queue-leader-locator", "least-leaders"));
                assertThat(response).is(ok());
                createdStreams.add(s);
              });

      Map<Integer, Integer> leaderCount = new HashMap<>();
      Map<String, StreamMetadata> metadata =
          client.metadata(createdStreams.toArray(new String[] {}));
      assertThat(metadata).hasSize(streamCount);

      metadata
          .values()
          .forEach(
              streamMetadata -> {
                assertThat(streamMetadata.isResponseOk()).isTrue();
                assertThat(streamMetadata.getLeader()).isNotNull();
                leaderCount.compute(
                    streamMetadata.getLeader().getPort(),
                    (port, value) -> value == null ? 1 : ++value);
              });
      assertThat(leaderCount).hasSize(clusterSize);
      leaderCount.values().forEach(count -> assertThat(count).isEqualTo(streamsByNode));
    } finally {
      createdStreams.forEach(
          s -> {
            Response response = client.delete(s);
            if (!response.isOk()) {
              LoggerFactory.getLogger(LeaderLocatorTest.class).warn("Error while deleting stream");
            }
          });
    }
  }
}
