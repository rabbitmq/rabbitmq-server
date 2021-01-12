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

import com.rabbitmq.stream.TestUtils.ClientFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class PrometheusHttpTest {

  static OkHttpClient httpClient = new OkHttpClient.Builder().build();
  ClientFactory cf;
  String stream;

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
    return "http://localhost:" + TestUtils.prometheusPort() + "/metrics" + endpoint;
  }

  static Metrics metrics() throws IOException {
    return parseMetrics(get(""));
  }

  static Metrics parseMetrics(String content) throws IOException {
    Metrics metrics = new Metrics();
    try (BufferedReader reader = new BufferedReader(new StringReader(content))) {
      String line;
      String type = null, name = null;
      Metric metric = null;
      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty() || !line.contains("rabbitmq_stream_")) {
          continue;
        }
        if (line.startsWith("# TYPE ")) {
          String[] nameType = line.replace("# TYPE ", "").split(" ");
          name = nameType[0];
          type = nameType[1];
        } else if (line.startsWith("# HELP ")) {
          String help = line.replace("# HELP ", "").replace(name + " ", "");
          metric = new Metric(name, type, help);
          metrics.add(metric);
        } else if (line.startsWith(name)) {
          Map<String, String> labels = Collections.emptyMap();
          if (line.contains("{")) {
            String l = line.substring(line.indexOf("{"), line.indexOf("}"));
            labels = Arrays.stream(l.split(",")).map(label -> label.trim().split("="))
                .collect(() -> new HashMap<>(),
                    (acc, keyValue) -> acc.put(keyValue[0], keyValue[1].replace("\"", "")),
                    (BiConsumer<Map<String, String>, Map<String, String>>) (stringStringHashMap, stringStringHashMap2) -> stringStringHashMap.putAll(stringStringHashMap2));

          }
          int value;
          try {
            value = Integer.valueOf(line.split(" ")[1]);
          } catch (NumberFormatException e) {
            value = 0;
          }
          metric.add(new MetricValue(value, labels));
        } else {
          throw new IllegalStateException("Cannot parse line: " + line);
        }
      }
    }

    return metrics;
  }

  @Test
  void aggregatedMetricsWithNoConnectionShouldReturnZero() throws Exception {
    Metrics metrics = metrics();
    System.out.println(metrics);
  }

  static class MetricValue {

    private final int value;
    private final Map<String, String> labels;

    MetricValue(int value, Map<String, String> labels) {
      this.value = value;
      this.labels = labels == null ? Collections.emptyMap() : labels;
    }

    @Override
    public String toString() {
      return "MetricValue{" +
          "value=" + value +
          ", labels=" + labels +
          '}';
    }
  }

  static class Metric {

    private final String name;
    private final String type;
    private final String help;
    private final List<MetricValue> values = new ArrayList<>();

    Metric(String name, String type, String help) {
      this.name = name;
      this.type = type;
      this.help = help;
    }

    void add(MetricValue value) {
      values.add(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Metric metric = (Metric) o;
      return name.equals(metric.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }

    @Override
    public String toString() {
      return "Metric{" +
          "name='" + name + '\'' +
          ", type='" + type + '\'' +
          ", help='" + help + '\'' +
          ", values=" + values +
          '}';
    }
  }

  static class Metrics {

    private final Map<String, Metric> metrics = new HashMap<>();

    void add(Metric metric) {
      this.metrics.put(metric.name, metric);
    }

    Metric get(String name) {
      return metrics.get(name);
    }

    @Override
    public String toString() {
      return "Metrics{" +
          "metrics=" + metrics +
          '}';
    }
  }
}
