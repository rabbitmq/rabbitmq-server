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
// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
//

package com.rabbitmq.stream;

import static com.rabbitmq.stream.TestUtils.booleanFalse;
import static com.rabbitmq.stream.TestUtils.booleanTrue;
import static com.rabbitmq.stream.TestUtils.isNull;
import static com.rabbitmq.stream.TestUtils.notNull;
import static com.rabbitmq.stream.TestUtils.waitUntil;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rabbitmq.stream.TestUtils.ClientFactory;
import com.rabbitmq.stream.TestUtils.TrustEverythingTrustManager;
import com.rabbitmq.stream.impl.Client;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class HttpTest {

  static OkHttpClient httpClient = httpClient("guest");
  static Gson gson = new GsonBuilder().create();
  EventLoopGroup eventLoopGroup;
  ClientFactory cf;
  String stream;

  static OkHttpClient httpClient(String usernamePassword) {
    return new OkHttpClient.Builder()
        .authenticator(TestUtils.authenticator(usernamePassword))
        .build();
  }

  static String get(String endpoint) throws IOException {
    return get(httpClient, endpoint);
  }

  static String get(OkHttpClient client, String endpoint) throws IOException {
    Request request = new Request.Builder().url(url(endpoint)).build();
    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);

      ResponseBody body = response.body();
      return body == null ? "" : body.string();
    }
  }

  static String url(String endpoint) {
    return "http://localhost:" + TestUtils.managementPort() + "/api" + endpoint;
  }

  @SuppressWarnings("unchecked")
  static Map<String, String> connectionDetails(Map<String, Object> parent) {
    return (Map<String, String>) parent.get("connection_details");
  }

  @SuppressWarnings("unchecked")
  static Map<String, String> queue(Map<String, Object> parent) {
    return (Map<String, String>) parent.get("queue");
  }

  static void delete(String endpoint) throws IOException {
    Request request = new Request.Builder().delete().url(url(endpoint)).build();
    try (Response response = httpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
    }
  }

  @SuppressWarnings("unchecked")
  static List<Map<String, Object>> toMaps(String json) {
    return Arrays.asList(gson.fromJson(json, Map[].class));
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> toMap(String json) {
    return gson.fromJson(json, Map.class);
  }

  static String connectionName(Client client) {
    InetSocketAddress localAddress = (InetSocketAddress) client.localAddress();
    InetSocketAddress remoteAddress = (InetSocketAddress) client.remoteAddress();
    return format("127.0.0.1:%d -> 127.0.0.1:%d", localAddress.getPort(), remoteAddress.getPort());
  }

  static List<Map<String, Object>> entities(List<Map<String, Object>> entities, Client client) {
    String connectionName = connectionName(client);
    return entities.stream()
        .filter(
            c ->
                c.get("connection_details") instanceof Map
                    && connectionName.equals(connectionDetails(c).get("name")))
        .collect(Collectors.toList());
  }

  static List<Map<String, Object>> entities(
      List<Map<String, Object>> entities, Predicate<Map<String, Object>> filter) {
    return entities.stream().filter(filter).collect(Collectors.toList());
  }

  static Map<String, Object> entity(
      List<Map<String, Object>> entities, Predicate<Map<String, Object>> filter) {
    return entities.stream().filter(filter).findFirst().orElse(Collections.emptyMap());
  }

  static TestRequest[] requests(TestRequest... requests) {
    return requests;
  }

  static TestRequest r(String endpoint, int expectedCount) {
    return new TestRequest(endpoint, expectedCount);
  }

  static Stream<Map<String, String>> subscriptionProperties() {
    return Stream.of(Collections.emptyMap(), map());
  }

  static Map<String, String> map() {
    Map<String, String> map = new LinkedHashMap<>();
    map.put("key1", "value1");
    map.put("key2", "value2");
    return map;
  }

  @Test
  void connections() throws Exception {
    Callable<List<Map<String, Object>>> request = () -> toMaps(get("/stream/connections"));
    int initialCount = request.call().size();
    String connectionProvidedName = UUID.randomUUID().toString();
    AtomicBoolean closed = new AtomicBoolean(false);
    Client client =
        cf.get(
            new ClientParameters()
                .clientProperty("connection_name", connectionProvidedName)
                .shutdownListener(shutdownContext -> closed.set(true)));

    waitUntil(() -> request.call().size() == initialCount + 1);

    Map<String, Object> c =
        request.call().stream()
            .filter(conn -> connectionProvidedName.equals(conn.get("user_provided_name")))
            .findFirst()
            .get();

    String connectionName = connectionName(client);
    assertThat(c).containsEntry("name", connectionName);

    assertThat(c)
        .hasEntrySatisfying("ssl", booleanFalse())
        .hasEntrySatisfying("ssl_cipher", isNull())
        .hasEntrySatisfying("ssl_hash", isNull())
        .hasEntrySatisfying("ssl_key_exchange", isNull())
        .hasEntrySatisfying("ssl_protocol", isNull());

    Callable<Map<String, Object>> cRequest =
        () -> toMap(get("/stream/connections/%2F/" + connectionName));
    // wait until some stats are in the response
    waitUntil(() -> cRequest.call().containsKey("recv_oct_details"));
    c = cRequest.call();

    Condition<Object> mapCondition = new Condition<>(e -> e instanceof Map, "Must be a map");
    assertThat(c)
        .hasEntrySatisfying("recv_oct_details", mapCondition)
        .hasEntrySatisfying("send_oct_details", mapCondition)
        .hasEntrySatisfying("garbage_collection", mapCondition)
        .hasEntrySatisfying("reductions_details", mapCondition);

    assertThat(closed.get()).isFalse();
    delete("/stream/connections/%2F/" + connectionName);
    waitUntil(closed::get);

    assertThatThrownBy(cRequest::call).isInstanceOf(IOException.class);
    waitUntil(() -> request.call().size() == initialCount);
  }

  @Test
  void tlsConnections() throws Exception {
    Callable<List<Map<String, Object>>> request = () -> toMaps(get("/stream/connections"));
    int initialCount = request.call().size();
    String connectionProvidedName = UUID.randomUUID().toString();
    try (Client client =
        new Client(
            new ClientParameters()
                .eventLoopGroup(this.eventLoopGroup)
                .sslContext(
                    SslContextBuilder.forClient()
                        .trustManager(new TrustEverythingTrustManager())
                        .build())
                .port(TestUtils.streamPortTls())
                .clientProperty("connection_name", connectionProvidedName))) {

      waitUntil(() -> request.call().size() == initialCount + 1);

      Map<String, Object> c =
          request.call().stream()
              .filter(conn -> connectionProvidedName.equals(conn.get("user_provided_name")))
              .findFirst()
              .get();

      String connectionName = connectionName(client);
      assertThat(c).containsEntry("name", connectionName);

      assertThat(c)
          .hasEntrySatisfying("ssl", booleanTrue())
          .hasEntrySatisfying("ssl_cipher", notNull())
          .hasEntrySatisfying("ssl_hash", notNull())
          .hasEntrySatisfying("ssl_key_exchange", notNull())
          .hasEntrySatisfying("ssl_protocol", notNull());
    }
    waitUntil(() -> request.call().size() == initialCount);
  }

  @Test
  void connectionConsumers() throws Exception {
    Callable<List<Map<String, Object>>> request = () -> toMaps(get("/stream/connections"));
    int initialCount = request.call().size();
    String s = UUID.randomUUID().toString();
    Client c1 = cf.get(new ClientParameters().virtualHost("vh1"));
    try {
      c1.create(s);
      assertThat(c1.subscribe((byte) 0, s, OffsetSpecification.first(), 10).isOk()).isTrue();
      assertThat(c1.subscribe((byte) 1, s, OffsetSpecification.first(), 5).isOk()).isTrue();
      Client c2 =
          cf.get(
              new ClientParameters()
                  .virtualHost("vh1")
                  .username("user-management")
                  .password("user-management"));
      assertThat(c2.subscribe((byte) 0, s, OffsetSpecification.first(), 10).isOk()).isTrue();
      waitUntil(() -> request.call().size() == initialCount + 2);

      Callable<Map<String, Object>> cRequest =
          () -> toMap(get("/stream/connections/vh1/" + connectionName(c1)));
      // wait until some stats are in the response
      waitUntil(() -> cRequest.call().containsKey("recv_oct_details"));

      Callable<List<Map<String, Object>>> consumersRequest =
          () -> toMaps(get("/stream/connections/vh1/" + connectionName(c1) + "/consumers"));
      List<Map<String, Object>> consumers = consumersRequest.call();

      assertThat(consumers).hasSize(2);
      consumers.forEach(
          c -> {
            assertThat(c)
                .containsKeys(
                    "subscription_id", "credits", "connection_details", "queue", "properties");
            assertThat(c)
                .extractingByKey("connection_details", as(MAP))
                .containsValue(connectionName(c1));
          });

      consumersRequest =
          () -> toMaps(get("/stream/connections/vh1/" + connectionName(c2) + "/consumers"));
      consumers = consumersRequest.call();
      assertThat(consumers).hasSize(1);
      assertThat(consumers.get(0))
          .extractingByKey("connection_details", as(MAP))
          .containsValue(connectionName(c2));

      assertThatThrownBy(
              () ->
                  get(
                      httpClient("user-management"),
                      "/stream/connections/vh1/" + connectionName(c1) + "/consumers"))
          .hasMessageContaining("401");
    } finally {
      c1.delete(s);
    }
  }

  @Test
  void connectionPublishers() throws Exception {
    Callable<List<Map<String, Object>>> request = () -> toMaps(get("/stream/connections"));
    int initialCount = request.call().size();
    String s = UUID.randomUUID().toString();
    Client c1 = cf.get(new ClientParameters().virtualHost("vh1"));
    try {
      c1.create(s);
      assertThat(c1.declarePublisher((byte) 0, null, s).isOk()).isTrue();
      assertThat(c1.declarePublisher((byte) 1, null, s).isOk()).isTrue();
      Client c2 =
          cf.get(
              new ClientParameters()
                  .virtualHost("vh1")
                  .username("user-management")
                  .password("user-management"));
      assertThat(c2.declarePublisher((byte) 0, null, s).isOk()).isTrue();
      waitUntil(() -> request.call().size() == initialCount + 2);

      Callable<Map<String, Object>> cRequest =
          () -> toMap(get("/stream/connections/vh1/" + connectionName(c1)));
      // wait until some stats are in the response
      waitUntil(() -> cRequest.call().containsKey("recv_oct_details"));

      Callable<List<Map<String, Object>>> publishersRequest =
          () -> toMaps(get("/stream/connections/vh1/" + connectionName(c1) + "/publishers"));
      List<Map<String, Object>> publishers = publishersRequest.call();

      assertThat(publishers).hasSize(2);
      publishers.forEach(
          c -> {
            assertThat(c)
                .containsKeys(
                    "publisher_id",
                    "reference",
                    "published",
                    "confirmed",
                    "errored",
                    "connection_details",
                    "queue");
            assertThat(c)
                .extractingByKey("connection_details", as(MAP))
                .containsValue(connectionName(c1));
          });

      publishersRequest =
          () -> toMaps(get("/stream/connections/vh1/" + connectionName(c2) + "/publishers"));
      publishers = publishersRequest.call();
      assertThat(publishers).hasSize(1);
      assertThat(publishers.get(0))
          .extractingByKey("connection_details", as(MAP))
          .containsValue(connectionName(c2));

      assertThatThrownBy(
              () ->
                  get(
                      httpClient("user-management"),
                      "/stream/connections/vh1/" + connectionName(c1) + "/publishers"))
          .hasMessageContaining("401");
    } finally {
      c1.delete(s);
    }
  }

  @Test
  void publishers() throws Exception {
    Callable<List<Map<String, Object>>> request = () -> toMaps(get("/stream/publishers"));
    int initialCount = request.call().size();
    String connectionProvidedName = UUID.randomUUID().toString();
    AtomicBoolean closed = new AtomicBoolean(false);
    Client client =
        cf.get(
            new ClientParameters()
                .clientProperty("connection_name", connectionProvidedName)
                .shutdownListener(shutdownContext -> closed.set(true)));

    client.declarePublisher((byte) 0, null, stream);
    waitUntil(() -> request.call().size() == initialCount + 1);
    assertThat(toMaps(get("/stream/publishers/%2F"))).hasSize(1);
    assertThat(toMaps(get("/stream/publishers/vh1"))).isEmpty();
    waitUntil(() -> entities(request.call(), client).size() == 1);

    Map<String, Object> publisher = entities(request.call(), client).get(0);
    assertThat(publisher.get("reference").toString()).isEmpty();
    assertThat(((Number) publisher.get("published")).intValue()).isEqualTo(0);
    assertThat(((Number) publisher.get("confirmed")).intValue()).isEqualTo(0);
    assertThat(((Number) publisher.get("errored")).intValue()).isEqualTo(0);
    assertThat(((Number) publisher.get("publisher_id")).intValue()).isEqualTo(0);
    assertThat(connectionDetails(publisher))
        .containsEntry("name", connectionName(client))
        .containsEntry("user", "guest")
        .containsKey("node");
    assertThat(queue(publisher)).containsEntry("name", stream).containsEntry("vhost", "/");

    client.publish(
        (byte) 0,
        Collections.singletonList(
            client.messageBuilder().addData("".getBytes(StandardCharsets.UTF_8)).build()));

    waitUntil(
        () -> ((Number) entities(request.call(), client).get(0).get("confirmed")).intValue() == 1);
    publisher = entities(request.call(), client).get(0);
    assertThat(((Number) publisher.get("published")).intValue()).isEqualTo(1);
    assertThat(((Number) publisher.get("confirmed")).intValue()).isEqualTo(1);

    client.declarePublisher((byte) 1, null, stream);
    waitUntil(() -> entities(request.call(), client).size() == 2);

    client.deletePublisher((byte) 0);
    waitUntil(() -> entities(request.call(), client).size() == 1);
    client.deletePublisher((byte) 1);
    waitUntil(() -> entities(request.call(), client).isEmpty());
  }

  @Test
  void publishersByStream() throws Exception {
    Callable<List<Map<String, Object>>> request =
        () -> toMaps(get("/stream/publishers/%2F/" + stream));
    int initialCount = request.call().size();
    String connectionProvidedName = UUID.randomUUID().toString();
    AtomicBoolean closed = new AtomicBoolean(false);
    Client client =
        cf.get(
            new ClientParameters()
                .clientProperty("connection_name", connectionProvidedName)
                .shutdownListener(shutdownContext -> closed.set(true)));

    String otherStream = UUID.randomUUID().toString();
    assertThat(client.create(otherStream).isOk()).isTrue();

    client.declarePublisher((byte) 0, null, stream);
    client.declarePublisher((byte) 1, null, otherStream);

    waitUntil(() -> toMaps(get("/stream/publishers/%2F")).size() == initialCount + 2);
    waitUntil(() -> request.call().size() == initialCount + 1);
    waitUntil(() -> entities(request.call(), client).size() == 1);

    Map<String, Object> publisher = entities(request.call(), client).get(0);
    assertThat(connectionDetails(publisher))
        .containsEntry("name", connectionName(client))
        .containsEntry("user", "guest")
        .containsKey("node");
    assertThat(queue(publisher)).containsEntry("name", stream).containsEntry("vhost", "/");

    Callable<List<Map<String, Object>>> requestOtherStream =
        () -> toMaps(get("/stream/publishers/%2F/" + otherStream));
    waitUntil(() -> entities(requestOtherStream.call(), client).size() == 1);

    publisher = entities(requestOtherStream.call(), client).get(0);
    assertThat(connectionDetails(publisher))
        .containsEntry("name", connectionName(client))
        .containsEntry("user", "guest")
        .containsKey("node");
    assertThat(queue(publisher)).containsEntry("name", otherStream).containsEntry("vhost", "/");

    client.deletePublisher((byte) 0);
    client.deletePublisher((byte) 1);
  }

  @ParameterizedTest
  @ValueSource(strings = {"foo"})
  @NullSource
  void publisherReference(String reference) throws Exception {
    Callable<List<Map<String, Object>>> request = () -> toMaps(get("/stream/publishers"));
    int initialCount = request.call().size();
    String connectionProvidedName = UUID.randomUUID().toString();
    AtomicBoolean closed = new AtomicBoolean(false);
    Client client =
        cf.get(
            new ClientParameters()
                .clientProperty("connection_name", connectionProvidedName)
                .shutdownListener(shutdownContext -> closed.set(true)));

    client.declarePublisher((byte) 0, reference, stream);
    waitUntil(() -> request.call().size() == initialCount + 1);
    waitUntil(() -> entities(request.call(), client).size() == 1);

    Map<String, Object> publisher = entities(request.call(), client).get(0);
    String publisherReference = (String) publisher.get("reference");
    if (reference == null || reference.isEmpty()) {
      assertThat(publisherReference).isEmpty();
    } else {
      assertThat(publisher.get("reference").toString()).isEqualTo(reference);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"foo"})
  @NullSource
  void publisherShouldBeDeletedAfterStreamDeletion(String reference) throws Exception {
    Callable<List<Map<String, Object>>> request = () -> toMaps(get("/stream/publishers"));
    int initialCount = request.call().size();
    String connectionProvidedName = UUID.randomUUID().toString();
    String s = UUID.randomUUID().toString();
    AtomicBoolean closed = new AtomicBoolean(false);
    Client client =
        cf.get(
            new ClientParameters()
                .clientProperty("connection_name", connectionProvidedName)
                .shutdownListener(shutdownContext -> closed.set(true)));

    client.create(s);
    client.declarePublisher((byte) 0, reference, s);
    waitUntil(() -> request.call().size() == initialCount + 1);
    waitUntil(() -> entities(request.call(), client).size() == 1);

    client.delete(s);
    waitUntil(() -> request.call().size() == 0);
  }

  @Test
  void consumerShouldBeDeletedAfterStreamDeletion() throws Exception {
    Callable<List<Map<String, Object>>> request = () -> toMaps(get("/stream/consumers"));
    int initialCount = request.call().size();
    String connectionProvidedName = UUID.randomUUID().toString();
    String s = UUID.randomUUID().toString();
    AtomicBoolean closed = new AtomicBoolean(false);
    Client client =
        cf.get(
            new ClientParameters()
                .clientProperty("connection_name", connectionProvidedName)
                .shutdownListener(shutdownContext -> closed.set(true)));

    client.create(s);
    client.subscribe((byte) 0, s, OffsetSpecification.first(), 10);
    waitUntil(() -> request.call().size() == initialCount + 1);
    waitUntil(() -> entities(request.call(), client).size() == 1);

    client.delete(s);
    waitUntil(() -> request.call().size() == initialCount);
  }

  @ParameterizedTest
  @MethodSource("subscriptionProperties")
  void consumers(Map<String, String> subscriptionProperties) throws Exception {
    Callable<List<Map<String, Object>>> request = () -> toMaps(get("/stream/consumers"));
    int initialCount = request.call().size();
    String connectionProvidedName = UUID.randomUUID().toString();
    AtomicBoolean closed = new AtomicBoolean(false);
    Client client =
        cf.get(
            new ClientParameters()
                .clientProperty("connection_name", connectionProvidedName)
                .chunkListener(
                    (client1, subscriptionId, offset, messageCount, dataSize) ->
                        client1.credit(subscriptionId, 1))
                .shutdownListener(shutdownContext -> closed.set(true)));

    client.subscribe((byte) 0, stream, OffsetSpecification.first(), 10, subscriptionProperties);
    waitUntil(() -> request.call().size() == initialCount + 1);
    waitUntil(() -> entities(request.call(), client).size() == 1);

    Map<String, Object> consumer = entities(request.call(), client).get(0);
    assertThat(((Number) consumer.get("credits")).intValue()).isEqualTo(10);
    assertThat(((Number) consumer.get("consumed")).intValue()).isEqualTo(0);
    assertThat(((Number) consumer.get("offset")).intValue()).isEqualTo(0);
    assertThat(((Number) consumer.get("subscription_id")).intValue()).isEqualTo(0);
    assertThat(consumer.get("properties")).isNotNull().isEqualTo(subscriptionProperties);

    assertThat(connectionDetails(consumer))
        .containsEntry("name", connectionName(client))
        .containsEntry("user", "guest")
        .containsKey("node");
    assertThat(queue(consumer)).containsEntry("name", stream).containsEntry("vhost", "/");

    client.subscribe((byte) 1, stream, OffsetSpecification.first(), 10);
    waitUntil(() -> entities(request.call(), client).size() == 2);

    client.unsubscribe((byte) 0);
    waitUntil(() -> entities(request.call(), client).size() == 1);

    int messageCount = 10_000;
    assertThat(client.declarePublisher((byte) 0, null, stream).isOk()).isTrue();
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                client.publish(
                    (byte) 0,
                    Collections.singletonList(
                        client
                            .messageBuilder()
                            .addData("".getBytes(StandardCharsets.UTF_8))
                            .build())));

    waitUntil(
        () -> {
          Map<String, Object> c = entities(request.call(), client).get(0);
          return ((Number) c.get("consumed")).intValue() == messageCount;
        });

    consumer = entities(request.call(), client).get(0);
    assertThat(((Number) consumer.get("consumed")).intValue()).isEqualTo(messageCount);
    assertThat(((Number) consumer.get("offset")).intValue()).isPositive();

    client.unsubscribe((byte) 1);
    waitUntil(() -> entities(request.call(), client).isEmpty());

    client.subscribe((byte) 0, stream, OffsetSpecification.next(), 10);
    waitUntil(() -> request.call().size() == initialCount + 1);
    waitUntil(() -> entities(request.call(), client).size() == 1);

    consumer = entities(request.call(), client).get(0);
    assertThat(((Number) consumer.get("consumed")).intValue()).isEqualTo(0);
    assertThat(((Number) consumer.get("offset")).intValue()).isEqualTo(messageCount);

    client.unsubscribe((byte) 0);
    waitUntil(() -> entities(request.call(), client).isEmpty());
  }

  @Test
  void permissions() throws Exception {
    String[][] vhostsUsers =
        new String[][] {
          {"/", "guest"},
          {"vh1", "user-management"},
          {"vh1", "user-management"},
          {"vh2", "guest"},
          {"vh2", "guest"},
        };
    Map<String, Client> vhostClients = new HashMap<>();
    List<Client> clients =
        Arrays.stream(vhostsUsers)
            .map(
                vhostUser -> {
                  Client c =
                      cf.get(
                          new ClientParameters()
                              .virtualHost(vhostUser[0])
                              .username(vhostUser[1])
                              .password(vhostUser[1]));
                  vhostClients.put(vhostUser[0], c);
                  return c;
                })
            .collect(Collectors.toList());

    List<String> nonDefaultVhosts =
        Arrays.stream(vhostsUsers)
            .map(vhostUser -> vhostUser[0])
            .filter(vhost -> !vhost.equals("/"))
            .distinct()
            .collect(Collectors.toList());
    nonDefaultVhosts.forEach(
        vhost -> {
          Client c = vhostClients.get(vhost);
          c.create(stream);
        });

    try {
      int entitiesPerConnection = 2;

      IntStream.range(0, entitiesPerConnection)
          .forEach(
              i ->
                  clients.forEach(
                      c -> {
                        c.subscribe((byte) i, stream, OffsetSpecification.first(), 10);
                        c.declarePublisher((byte) i, null, stream);
                      }));
      Callable<List<Map<String, Object>>> allConnectionsRequest =
          () -> toMaps(get("/stream/connections"));
      int initialCount = allConnectionsRequest.call().size();
      waitUntil(() -> allConnectionsRequest.call().size() == initialCount + 5);

      String vhost1ConnectionName =
          toMaps(get("/stream/connections/vh1")).stream()
              .filter(c -> "vh1".equals(c.get("vhost")))
              .map(c -> c.get("name").toString())
              .findFirst()
              .get();

      String vhost2ConnectionName =
          toMaps(get("/stream/connections/vh2")).stream()
              .filter(c -> "vh2".equals(c.get("vhost")))
              .map(c -> c.get("name").toString())
              .findFirst()
              .get();

      PermissionsTestConfiguration[] testConfigurations =
          new PermissionsTestConfiguration[] {
            new PermissionsTestConfiguration(
                "guest",
                "/connections",
                requests(r("", 5), r("/%2f", 1), r("/vh1", 2), r("/vh2", 2)),
                "vh1/" + vhost1ConnectionName,
                true,
                "vh2/" + vhost2ConnectionName,
                true),
            new PermissionsTestConfiguration(
                "user-monitoring",
                "/connections",
                requests(r("", 5), r("/%2f", 1), r("/vh1", 2), r("/vh2", 2)),
                "vh1/" + vhost1ConnectionName,
                true,
                "vh2/" + vhost2ConnectionName,
                true),
            new PermissionsTestConfiguration(
                "user-management",
                "/connections",
                requests(r("", 2), r("/%2f", -1), r("/vh1", 2), r("/vh2", -1)),
                "vh1/" + vhost1ConnectionName,
                true,
                "vh2/" + vhost2ConnectionName,
                false),
            new PermissionsTestConfiguration(
                "guest",
                "",
                requests(
                    r("/consumers", vhostsUsers.length * entitiesPerConnection),
                    r("/publishers", vhostsUsers.length * entitiesPerConnection),
                    r("/consumers/%2f", entitiesPerConnection),
                    r("/publishers/%2f", entitiesPerConnection),
                    r("/consumers/vh1", entitiesPerConnection * 2),
                    r("/publishers/vh1", entitiesPerConnection * 2))),
            new PermissionsTestConfiguration(
                "user-management",
                "",
                requests(
                    r("/consumers", entitiesPerConnection * 2), // only their connections
                    r("/publishers", entitiesPerConnection * 2), // only their connections
                    r("/consumers/vh1", entitiesPerConnection * 2),
                    r("/publishers/vh1", entitiesPerConnection * 2),
                    r("/consumers/vh2", 0),
                    r("/consumers/vh2", 0)))
          };

      for (PermissionsTestConfiguration configuration : testConfigurations) {
        OkHttpClient client = httpClient(configuration.user);
        for (TestRequest request : configuration.requests) {
          if (request.expectedCount >= 0) {
            assertThat(toMaps(get(client, "/stream" + configuration.endpoint + request.endpoint)))
                .hasSize(request.expectedCount);
          } else {
            assertThatThrownBy(
                    () ->
                        toMaps(get(client, "/stream" + configuration.endpoint + request.endpoint)))
                .hasMessageContaining("401");
          }
        }
        for (Entry<String, Boolean> request : configuration.vhostConnections.entrySet()) {
          if (request.getValue()) {
            Condition<Object> connNameCondition =
                new Condition<>(
                    e -> request.getKey().endsWith(e.toString()), "connection name must match");
            assertThat(toMap(get(client, "/stream/connections/" + request.getKey())))
                .hasEntrySatisfying("name", connNameCondition);
          } else {
            assertThatThrownBy(() -> toMap(get(client, "/stream/connections/" + request.getKey())))
                .hasMessageContaining("401");
          }
        }
      }

      clients.forEach(Client::close);
      waitUntil(() -> allConnectionsRequest.call().size() == initialCount);
    } finally {
      nonDefaultVhosts.forEach(
          vhost -> {
            Client c = cf.get(new ClientParameters().virtualHost(vhost));
            c.delete(stream);
          });
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "/stream/connections/%2F/foo-connection-name",
        "/stream/connections/foo-virtual-host",
        "/stream/connections/foo-virtual-host/foo-connection-name",
        "/stream/connections/%2F/foo-connection-name/consumers",
        "/stream/connections/%2F/foo-connection-name/publishers",
        "/stream/consumers/foo-virtual-host",
        "/stream/publishers/foo-virtual-host",
        "/stream/publishers/foo-virtual-host",
        "/stream/publishers/%2F/foo-stream"
      })
  void shouldReturnNotFound(String endpoint) {
    assertThatThrownBy(() -> get(endpoint)).hasMessageContaining("404");
  }

  @ParameterizedTest
  @MethodSource("subscriptionProperties")
  @SuppressWarnings("unchecked")
  void streamConsumersShouldShowUpAsRegularConsumers(Map<String, String> subscriptionProperties)
      throws Exception {
    Callable<List<Map<String, Object>>> consumersRequest = () -> toMaps(get("/consumers"));
    int initialCount = consumersRequest.call().size();
    String connectionProvidedName = UUID.randomUUID().toString();
    String s = UUID.randomUUID().toString();
    AtomicBoolean closed = new AtomicBoolean(false);
    Client client =
        cf.get(
            new ClientParameters()
                .clientProperty("connection_name", connectionProvidedName)
                .shutdownListener(shutdownContext -> closed.set(true)));

    try {

      client.create(s);
      client.subscribe((byte) 0, s, OffsetSpecification.first(), 10, subscriptionProperties);
      waitUntil(() -> consumersRequest.call().size() == initialCount + 1);

      String consumerTagPrefix = "stream.subid-";
      Map<String, Object> consumersConsumer =
          entity(
              consumersRequest.call(),
              m ->
                  m.get("consumer_tag").toString().startsWith(consumerTagPrefix)
                      && m.get("channel_details") != null
                      && m.get("channel_details") instanceof Map
                      && connectionName(client)
                          .equals(
                              ((Map<String, Object>) m.get("channel_details"))
                                  .get("connection_name")));

      Callable<Map<String, Object>> queueRequest = () -> toMap(get("/queues/%2F/" + s));
      Map<String, Object> queueDetails = queueRequest.call();

      assertThat(queueDetails).containsKey("consumer_details");
      assertThat(queueDetails.get("consumer_details")).isInstanceOf(List.class).asList().hasSize(1);
      Map<String, Object> queueConsumer =
          ((List<Map<String, Object>>) queueDetails.get("consumer_details")).get(0);

      Stream.of(consumersConsumer, queueConsumer)
          .forEach(
              consumer -> {
                assertThat(consumer)
                    .isNotNull()
                    .containsEntry("ack_required", false)
                    .containsEntry("active", true)
                    .containsEntry("activity_status", "up")
                    .containsEntry("consumer_tag", consumerTagPrefix + "0")
                    .containsEntry("exclusive", false)
                    .containsEntry("arguments", subscriptionProperties)
                    .hasEntrySatisfying(
                        "prefetch_count", o -> assertThat(((Number) o).intValue()).isZero());

                Map<String, String> queue = (Map<String, String>) consumer.get("queue");
                assertThat(queue).isNotNull().containsEntry("name", s);

                Map<String, Object> channel = (Map<String, Object>) consumer.get("channel_details");
                assertThat(channel)
                    .isNotNull()
                    .containsEntry("connection_name", connectionName(client))
                    .containsEntry("name", "")
                    .hasEntrySatisfying("number", o -> assertThat(((Number) o).intValue()).isZero())
                    .containsEntry("user", "guest")
                    .containsKeys("node", "peer_host", "peer_port");
              });
      Client.Response response = client.unsubscribe((byte) 0);
      assertThat(response.isOk()).isTrue();
      waitUntil(() -> consumersRequest.call().size() == initialCount);
    } finally {
      client.delete(s);
    }
  }

  static class PermissionsTestConfiguration {
    final String user;
    final String endpoint;
    final TestRequest[] requests;
    final Map<String, Boolean> vhostConnections;

    PermissionsTestConfiguration(
        String user, String endpoint, TestRequest[] requests, Object... vhostConnections) {
      this.user = user;
      this.endpoint = endpoint;
      this.requests = requests;
      this.vhostConnections = new LinkedHashMap<>();
      for (int i = 0; i < vhostConnections.length; i = i + 2) {
        this.vhostConnections.put(
            vhostConnections[i].toString(), (Boolean) vhostConnections[i + 1]);
      }
    }
  }

  static class TestRequest {
    final String endpoint;
    final int expectedCount;

    TestRequest(String endpoint, int expectedCount) {
      this.endpoint = endpoint;
      this.expectedCount = expectedCount;
    }
  }
}
