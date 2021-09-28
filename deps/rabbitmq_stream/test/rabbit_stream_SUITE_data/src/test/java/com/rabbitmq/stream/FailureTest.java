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
import static org.assertj.core.api.Assertions.fail;

import com.rabbitmq.stream.codec.WrapperMessageBuilder;
import com.rabbitmq.stream.impl.Client;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class FailureTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(FailureTest.class);

  TestUtils.ClientFactory cf;
  String stream;
  ExecutorService executorService;

  static void wait(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterEach
  void tearDown() {
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  @Test
  void leaderFailureWhenPublisherConnectedToReplica() throws Exception {
    Set<String> messages = new HashSet<>();
    Client client = cf.get(new Client.ClientParameters().port(TestUtils.streamPortNode1()));
    Map<String, Client.StreamMetadata> metadata = client.metadata(stream);
    Client.StreamMetadata streamMetadata = metadata.get(stream);
    assertThat(streamMetadata).isNotNull();

    TestUtils.waitUntil(() -> client.metadata(stream).get(stream).getReplicas().size() == 2);

    streamMetadata = client.metadata(stream).get(stream);
    assertThat(streamMetadata.getLeader().getPort()).isEqualTo(TestUtils.streamPortNode1());
    assertThat(streamMetadata.getReplicas()).isNotEmpty();

    Client.Broker replica = streamMetadata.getReplicas().get(0);
    assertThat(replica.getPort()).isNotEqualTo(TestUtils.streamPortNode1());

    AtomicReference<CountDownLatch> confirmLatch = new AtomicReference<>(new CountDownLatch(1));

    CountDownLatch metadataLatch = new CountDownLatch(1);
    Client publisher =
        cf.get(
            new Client.ClientParameters()
                .port(replica.getPort())
                .metadataListener((stream, code) -> metadataLatch.countDown())
                .publishConfirmListener(
                    (publisherId, publishingId) -> confirmLatch.get().countDown()));
    String message = "all nodes available";
    messages.add(message);
    publisher.declarePublisher((byte) 1, null, stream);
    publisher.publish(
        (byte) 1,
        Collections.singletonList(
            publisher.messageBuilder().addData(message.getBytes(StandardCharsets.UTF_8)).build()));
    assertThat(confirmLatch.get().await(10, TimeUnit.SECONDS)).isTrue();
    confirmLatch.set(null);

    try {
      Host.rabbitmqctl("stop_app");
      try {
        cf.get(new Client.ClientParameters().port(TestUtils.streamPortNode1()));
        fail("Node app stopped, connecting should not be possible");
      } catch (Exception e) {
        // OK
      }

      assertThat(metadataLatch.await(10, TimeUnit.SECONDS)).isTrue();

      // wait until there's a new leader
      TestUtils.waitAtMost(
          Duration.ofSeconds(10),
          () -> {
            Client.StreamMetadata m = publisher.metadata(stream).get(stream);
            return m.getLeader() != null && m.getLeader().getPort() != TestUtils.streamPortNode1();
          });

      confirmLatch.set(new CountDownLatch(1));
      message = "2 nodes available";
      messages.add(message);

      publisher.declarePublisher((byte) 1, null, stream);
      publisher.publish(
          (byte) 1,
          Collections.singletonList(
              publisher
                  .messageBuilder()
                  .addData(message.getBytes(StandardCharsets.UTF_8))
                  .build()));
      assertThat(confirmLatch.get().await(10, TimeUnit.SECONDS)).isTrue();
      confirmLatch.set(null);
    } finally {
      Host.rabbitmqctl("start_app");
    }

    // wait until all the replicas are there
    TestUtils.waitAtMost(
        Duration.ofSeconds(10),
        () -> {
          LOGGER.info("Getting metadata for {}", stream);
          Client.StreamMetadata m = publisher.metadata(stream).get(stream);
          LOGGER.info("Metadata for {} (expecting 2 replicas): {}", stream, m);
          return m.getReplicas().size() == 2;
        });

    confirmLatch.set(new CountDownLatch(1));
    message = "all nodes are back";
    messages.add(message);
    publisher.publish(
        (byte) 1,
        Collections.singletonList(
            publisher.messageBuilder().addData(message.getBytes(StandardCharsets.UTF_8)).build()));
    assertThat(confirmLatch.get().await(10, TimeUnit.SECONDS)).isTrue();
    confirmLatch.set(null);

    CountDownLatch consumeLatch = new CountDownLatch(messages.size());
    Set<String> bodies = ConcurrentHashMap.newKeySet();
    Client consumer =
        cf.get(
            new Client.ClientParameters()
                .port(TestUtils.streamPortNode1())
                .messageListener(
                    (subscriptionId, offset, chunkTimestamp, msg) -> {
                      bodies.add(new String(msg.getBodyAsBinary(), StandardCharsets.UTF_8));
                      consumeLatch.countDown();
                    }));

    TestUtils.waitAtMost(
        Duration.ofSeconds(5),
        () -> {
          Client.Response response =
              consumer.subscribe((byte) 1, stream, OffsetSpecification.first(), 10);
          return response.isOk();
        });
    assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(bodies).hasSameSizeAs(messages).containsAll(messages);
  }

  @Test
  void noLostConfirmedMessagesWhenLeaderGoesAway() throws Exception {
    executorService = Executors.newCachedThreadPool();
    Client client = cf.get(new Client.ClientParameters().port(TestUtils.streamPortNode1()));
    Map<String, Client.StreamMetadata> metadata = client.metadata(stream);
    Client.StreamMetadata streamMetadata = metadata.get(stream);
    assertThat(streamMetadata).isNotNull();

    assertThat(streamMetadata.getLeader()).isNotNull();
    assertThat(streamMetadata.getLeader().getPort()).isEqualTo(TestUtils.streamPortNode1());

    Map<Long, Message> published = new ConcurrentHashMap<>();
    Set<Message> confirmed = ConcurrentHashMap.newKeySet();

    Client.PublishConfirmListener publishConfirmListener =
        (publisherId, publishingId) -> {
          Message confirmedMessage;
          int attempts = 0;
          while ((confirmedMessage = published.remove(publishingId)) == null && attempts < 10) {
            wait(Duration.ofMillis(5));
            attempts++;
          }
          confirmed.add(confirmedMessage);
        };

    AtomicLong generation = new AtomicLong(0);
    AtomicLong sequence = new AtomicLong(0);
    AtomicBoolean connected = new AtomicBoolean(true);
    AtomicReference<Client> publisher = new AtomicReference<>();
    CountDownLatch reconnectionLatch = new CountDownLatch(1);
    AtomicReference<Client.ShutdownListener> shutdownListenerReference = new AtomicReference<>();
    Client.ShutdownListener shutdownListener =
        shutdownContext -> {
          if (shutdownContext.getShutdownReason()
              == Client.ShutdownContext.ShutdownReason.UNKNOWN) {
            // avoid long-running task in the IO thread
            executorService.submit(
                () -> {
                  connected.set(false);

                  Client locator =
                      cf.get(new Client.ClientParameters().port(TestUtils.streamPortNode2()));
                  // wait until there's a new leader
                  try {
                    TestUtils.waitAtMost(
                        Duration.ofSeconds(5),
                        () -> {
                          Client.StreamMetadata m = locator.metadata(stream).get(stream);
                          return m.getLeader() != null
                              && m.getLeader().getPort() != TestUtils.streamPortNode1();
                        });
                  } catch (Throwable e) {
                    reconnectionLatch.countDown();
                    return;
                  }

                  int newLeaderPort = locator.metadata(stream).get(stream).getLeader().getPort();
                  Client newPublisher =
                      cf.get(
                          new Client.ClientParameters()
                              .port(newLeaderPort)
                              .shutdownListener(shutdownListenerReference.get())
                              .publishConfirmListener(publishConfirmListener));

                  generation.incrementAndGet();
                  published.clear();
                  newPublisher.declarePublisher((byte) 1, null, stream);
                  publisher.set(newPublisher);
                  connected.set(true);

                  reconnectionLatch.countDown();
                });
          }
        };
    shutdownListenerReference.set(shutdownListener);

    client =
        cf.get(
            new Client.ClientParameters()
                .port(streamMetadata.getLeader().getPort())
                .shutdownListener(shutdownListener)
                .publishConfirmListener(publishConfirmListener));

    client.declarePublisher((byte) 1, null, stream);
    publisher.set(client);

    AtomicBoolean keepPublishing = new AtomicBoolean(true);

    executorService.submit(
        () -> {
          while (keepPublishing.get()) {
            if (connected.get()) {
              Message message =
                  publisher
                      .get()
                      .messageBuilder()
                      .properties()
                      .messageId(sequence.getAndIncrement())
                      .messageBuilder()
                      .applicationProperties()
                      .entry("generation", generation.get())
                      .messageBuilder()
                      .build();
              try {
                long publishingId =
                    publisher.get().publish((byte) 1, Collections.singletonList(message)).get(0);
                published.put(publishingId, message);
              } catch (Exception e) {
                // keep going
              }
              wait(Duration.ofMillis(10));
            } else {
              wait(Duration.ofSeconds(1));
            }
          }
        });

    // let's publish for a bit of time
    Thread.sleep(2000);

    assertThat(confirmed).isNotEmpty();
    int confirmedCount = confirmed.size();

    try {
      Host.rabbitmqctl("stop_app");

      assertThat(reconnectionLatch.await(10, TimeUnit.SECONDS)).isTrue();

      // let's publish for a bit of time
      Thread.sleep(2000);

    } finally {
      Host.rabbitmqctl("start_app");
    }
    assertThat(confirmed).hasSizeGreaterThan(confirmedCount);
    confirmedCount = confirmed.size();

    Client metadataClient = cf.get(new Client.ClientParameters().port(TestUtils.streamPortNode2()));
    // wait until all the replicas are there
    TestUtils.waitAtMost(
        Duration.ofSeconds(5),
        () -> {
          Client.StreamMetadata m = metadataClient.metadata(stream).get(stream);
          return m.getReplicas().size() == 2;
        });

    // let's publish for a bit of time
    Thread.sleep(2000);

    assertThat(confirmed).hasSizeGreaterThan(confirmedCount);

    keepPublishing.set(false);

    Queue<Message> consumed = new ConcurrentLinkedQueue<>();
    Set<Long> generations = ConcurrentHashMap.newKeySet();
    CountDownLatch consumedLatch = new CountDownLatch(1);
    Client.StreamMetadata m = metadataClient.metadata(stream).get(stream);
    Client consumer =
        cf.get(
            new Client.ClientParameters()
                .port(m.getReplicas().get(0).getPort())
                .chunkListener(
                    (client1, subscriptionId, offset, messageCount, dataSize) ->
                        client1.credit(subscriptionId, 1))
                .messageListener(
                    (subscriptionId, offset, chunkTimestamp, message) -> {
                      consumed.add(message);
                      generations.add((Long) message.getApplicationProperties().get("generation"));
                      if (consumed.size() == confirmed.size()) {
                        consumedLatch.countDown();
                      }
                    }));

    Client.Response response =
        consumer.subscribe((byte) 1, stream, OffsetSpecification.first(), 10);
    assertThat(response.isOk()).isTrue();

    assertThat(consumedLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(generations).hasSize(2).contains(0L, 1L);
    assertThat(consumed).hasSizeGreaterThanOrEqualTo(confirmed.size());
    long lastMessageId = -1;
    for (Message message : consumed) {
      long messageId = message.getProperties().getMessageIdAsLong();
      assertThat(messageId).isGreaterThanOrEqualTo(lastMessageId);
      lastMessageId = messageId;
    }
    assertThat(lastMessageId).isPositive().isLessThanOrEqualTo(sequence.get());
  }

  @Test
  void consumerReattachesToOtherReplicaWhenReplicaGoesAway() throws Exception {
    executorService = Executors.newCachedThreadPool();
    Client metadataClient = cf.get(new Client.ClientParameters().port(TestUtils.streamPortNode1()));
    Map<String, Client.StreamMetadata> metadata = metadataClient.metadata(stream);
    Client.StreamMetadata streamMetadata = metadata.get(stream);
    assertThat(streamMetadata).isNotNull();

    TestUtils.waitUntil(
        () -> metadataClient.metadata(stream).get(stream).getReplicas().size() == 2);

    metadata = metadataClient.metadata(stream);
    streamMetadata = metadata.get(stream);
    assertThat(streamMetadata.getLeader()).isNotNull();
    assertThat(streamMetadata.getLeader().getPort()).isEqualTo(TestUtils.streamPortNode1());

    Map<Long, Message> published = new ConcurrentHashMap<>();
    Set<Message> confirmed = ConcurrentHashMap.newKeySet();
    Set<Long> confirmedIds = ConcurrentHashMap.newKeySet();
    Client.PublishConfirmListener publishConfirmListener =
        (publisherId, publishingId) -> {
          Message confirmedMessage;
          int attempts = 0;
          while ((confirmedMessage = published.remove(publishingId)) == null && attempts < 10) {
            wait(Duration.ofMillis(5));
            attempts++;
          }
          confirmed.add(confirmedMessage);
          confirmedIds.add(confirmedMessage.getProperties().getMessageIdAsLong());
        };

    Client publisher =
        cf.get(
            new Client.ClientParameters()
                .port(streamMetadata.getLeader().getPort())
                .publishConfirmListener(publishConfirmListener));

    publisher.declarePublisher((byte) 1, null, stream);
    AtomicLong generation = new AtomicLong(0);
    AtomicLong sequence = new AtomicLong(0);
    AtomicBoolean keepPublishing = new AtomicBoolean(true);
    CountDownLatch publishingLatch = new CountDownLatch(1);

    executorService.submit(
        () -> {
          while (keepPublishing.get()) {
            Message message =
                new WrapperMessageBuilder()
                    .properties()
                    .messageId(sequence.getAndIncrement())
                    .messageBuilder()
                    .applicationProperties()
                    .entry("generation", generation.get())
                    .messageBuilder()
                    .build();
            try {
              long publishingId =
                  publisher.publish((byte) 1, Collections.singletonList(message)).get(0);
              published.put(publishingId, message);
            } catch (Exception e) {
              // keep going
            }
            wait(Duration.ofMillis(10));
          }
          publishingLatch.countDown();
        });

    Queue<Message> consumed = new ConcurrentLinkedQueue<>();

    Client.Broker replica =
        streamMetadata.getReplicas().stream()
            .filter(broker -> broker.getPort() == TestUtils.streamPortNode2())
            .findFirst()
            .orElseThrow(() -> new NoSuchElementException());

    AtomicLong lastProcessedOffset = new AtomicLong(-1);
    Set<Long> generations = ConcurrentHashMap.newKeySet();
    Set<Long> consumedIds = ConcurrentHashMap.newKeySet();
    Client.MessageListener messageListener =
        (subscriptionId, offset, chunkTimestamp, message) -> {
          consumed.add(message);
          generations.add((Long) message.getApplicationProperties().get("generation"));
          consumedIds.add(message.getProperties().getMessageIdAsLong());
          lastProcessedOffset.set(offset);
        };

    CountDownLatch reconnectionLatch = new CountDownLatch(1);
    AtomicReference<Client.ShutdownListener> shutdownListenerReference = new AtomicReference<>();
    Client.ShutdownListener shutdownListener =
        shutdownContext -> {
          if (shutdownContext.getShutdownReason()
              == Client.ShutdownContext.ShutdownReason.UNKNOWN) {
            // avoid long-running task in the IO thread
            executorService.submit(
                () -> {
                  Client.StreamMetadata m = metadataClient.metadata(stream).get(stream);
                  int newReplicaPort = m.getReplicas().get(0).getPort();

                  Client newConsumer =
                      cf.get(
                          new Client.ClientParameters()
                              .port(newReplicaPort)
                              .shutdownListener(shutdownListenerReference.get())
                              .chunkListener(
                                  (client1, subscriptionId, offset, messageCount, dataSize) ->
                                      client1.credit(subscriptionId, 1))
                              .messageListener(messageListener));

                  newConsumer.subscribe(
                      (byte) 1,
                      stream,
                      OffsetSpecification.offset(lastProcessedOffset.get() + 1),
                      10);

                  generation.incrementAndGet();
                  reconnectionLatch.countDown();
                });
          }
        };
    shutdownListenerReference.set(shutdownListener);

    Client consumer =
        cf.get(
            new Client.ClientParameters()
                .port(replica.getPort())
                .shutdownListener(shutdownListener)
                .chunkListener(
                    (client1, subscriptionId, offset, messageCount, dataSize) ->
                        client1.credit(subscriptionId, 1))
                .messageListener(messageListener));

    Client.Response response =
        consumer.subscribe((byte) 1, stream, OffsetSpecification.first(), 10);
    assertThat(response.isOk()).isTrue();

    // let's publish for a bit of time
    Thread.sleep(2000);

    assertThat(confirmed).isNotEmpty();
    assertThat(consumed).isNotEmpty();
    int confirmedCount = confirmed.size();

    try {
      Host.rabbitmqctl("stop_app", Host.node2name());

      assertThat(reconnectionLatch.await(10, TimeUnit.SECONDS)).isTrue();

      // let's publish for a bit of time
      Thread.sleep(2000);

    } finally {
      Host.rabbitmqctl("start_app", Host.node2name());
    }
    assertThat(confirmed).hasSizeGreaterThan(confirmedCount);
    confirmedCount = confirmed.size();

    // wait until all the replicas are there
    TestUtils.waitAtMost(
        Duration.ofSeconds(10),
        () -> {
          Client.StreamMetadata m = metadataClient.metadata(stream).get(stream);
          return m.getReplicas().size() == 2;
        });

    // let's publish for a bit of time
    Thread.sleep(2000);

    assertThat(confirmed).hasSizeGreaterThan(confirmedCount);

    keepPublishing.set(false);

    assertThat(publishingLatch.await(5, TimeUnit.SECONDS)).isTrue();

    TestUtils.waitAtMost(Duration.ofSeconds(5), () -> consumed.size() >= confirmed.size());

    assertThat(generations).hasSize(2).contains(0L, 1L);
    assertThat(consumed).hasSizeGreaterThanOrEqualTo(confirmed.size());
    long lastMessageId = -1;
    for (Message message : consumed) {
      long messageId = message.getProperties().getMessageIdAsLong();
      assertThat(messageId).isGreaterThanOrEqualTo(lastMessageId);
      lastMessageId = messageId;
    }
    assertThat(lastMessageId).isPositive().isLessThanOrEqualTo(sequence.get());

    confirmedIds.forEach(confirmedId -> assertThat(consumedIds).contains(confirmedId));
  }
}
