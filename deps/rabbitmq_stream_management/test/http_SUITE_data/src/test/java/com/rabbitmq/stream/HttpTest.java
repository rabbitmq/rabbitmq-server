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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class HttpTest {

  static OkHttpClient httpClient =
      new OkHttpClient.Builder()
          .authenticator(
              (route, response) ->
                  response
                      .request()
                      .newBuilder()
                      .header("Authorization", Credentials.basic("guest", "guest"))
                      .build())
          .build();
  static Gson gson = new GsonBuilder().create();
  ClientFactory cf;

  static String get(String endpoint) throws IOException {
    Request request =
        new Request.Builder()
            .url("http://localhost:" + TestUtils.managementPort() + "/api" + endpoint)
            .build();
    try (Response response = httpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);

      String body = response.body().string();
      return body;
    }
  }

  static void delete(String endpoint) throws IOException {
    Request request =
        new Request.Builder()
            .delete()
            .url("http://localhost:" + TestUtils.managementPort() + "/api" + endpoint)
            .build();
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
  void http() throws Exception {
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
  }
}
