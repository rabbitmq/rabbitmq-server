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

import static com.rabbitmq.stream.TestUtils.waitUntil;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rabbitmq.stream.TestUtils.ClientFactory;
import com.rabbitmq.stream.impl.Client;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class HttpTest {

  static OkHttpClient httpClient = httpClient("guest");
  static Gson gson = new GsonBuilder().create();
  ClientFactory cf;

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

      String body = response.body().string();
      return body;
    }
  }

  static String url(String endpoint) {
    return "http://localhost:" + TestUtils.managementPort() + "/api" + endpoint;
  }

  static void delete(String endpoint) throws IOException {
    Request request = new Request.Builder().delete().url(url(endpoint)).build();
    try (Response response = httpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
    }
  }

  static List<Map<String, Object>> toMaps(String json) {
    return Arrays.asList(gson.fromJson(json, Map[].class));
  }

  static Map<String, Object> toMap(String json) {
    return gson.fromJson(json, Map.class);
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

    InetSocketAddress localAddress = (InetSocketAddress) client.localAddress();
    InetSocketAddress remoteAddress = (InetSocketAddress) client.remoteAddress();
    String connectionName =
        format("127.0.0.1:%d -> 127.0.0.1:%d", localAddress.getPort(), remoteAddress.getPort());
    assertThat(c).containsEntry("name", connectionName);

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
    waitUntil(() -> closed.get());

    assertThatThrownBy(() -> cRequest.call()).isInstanceOf(IOException.class);
    waitUntil(() -> request.call().size() == initialCount);
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
    List<Client> clients =
        Arrays.stream(vhostsUsers)
            .map(
                vhostUser ->
                    cf.get(
                        new ClientParameters()
                            .virtualHost(vhostUser[0])
                            .username(vhostUser[1])
                            .password(vhostUser[1])))
            .collect(Collectors.toList());
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

    class TestConfiguration {
      final String user;
      final Map<String, Integer> connectionRequests;
      final Map<String, Boolean> vhostConnections;

      TestConfiguration(String user, Object[] connectionRequests, Object... vhostConnections) {
        this.user = user;
        this.connectionRequests = new LinkedHashMap<>(connectionRequests.length / 2);
        for (int i = 0; i < connectionRequests.length; i = i + 2) {
          this.connectionRequests.put(
              connectionRequests[i].toString(), (Integer) connectionRequests[i + 1]);
        }
        this.vhostConnections = new LinkedHashMap<>();
        for (int i = 0; i < vhostConnections.length; i = i + 2) {
          this.vhostConnections.put(
              vhostConnections[i].toString(), (Boolean) vhostConnections[i + 1]);
        }
      }
    }

    TestConfiguration[] testConfigurations =
        new TestConfiguration[] {
          new TestConfiguration(
              "guest",
              new Object[] {"", 5, "/%2f", 1, "/vh1", 2, "/vh2", 2},
              "vh1/" + vhost1ConnectionName,
              true,
              "vh2/" + vhost2ConnectionName,
              true),
          new TestConfiguration(
              "user-monitoring",
              new Object[] {"", 5, "/%2f", 1, "/vh1", 2, "/vh2", 2},
              "vh1/" + vhost1ConnectionName,
              true,
              "vh2/" + vhost2ConnectionName,
              true),
          new TestConfiguration(
              "user-management",
              new Object[] {"", 2, "/%2f", -1, "/vh1", 2, "/vh2", -1},
              "vh1/" + vhost1ConnectionName,
              true,
              "vh2/" + vhost2ConnectionName,
              false)
        };

    for (TestConfiguration configuration : testConfigurations) {
      OkHttpClient client = httpClient(configuration.user);
      for (Entry<String, Integer> request : configuration.connectionRequests.entrySet()) {
        if (request.getValue() >= 0) {
          assertThat(toMaps(get(client, "/stream/connections" + request.getKey())))
              .hasSize(request.getValue());
        } else {
          assertThatThrownBy(() -> toMaps(get(client, "/stream/connections" + request.getKey())))
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

    clients.forEach(client -> client.close());
    waitUntil(() -> allConnectionsRequest.call().size() == initialCount);
  }
}
