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
// Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
//

package com.rabbitmq.stream;

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

public class MetricsUtils {

  static final String METRIC_PREFIX = "rabbitmq_stream_";
  static final String METRIC_PUBLISHERS = "publishers";
  static final String METRIC_PUBLISHERS_PUBLISHED = "publishers_messages_published_total";
  static final String METRIC_PUBLISHERS_CONFIRMED = "publishers_messages_confirmed_total";
  static final String METRIC_PUBLISHERS_ERRORED = "publishers_messages_errored_total";
  static final String METRIC_CONSUMERS = "consumers";
  static final String METRIC_CONSUMERS_CONSUMED = "consumers_messages_consumed_total";
  static final List<String> METRICS =
      Collections.unmodifiableList(
          Arrays.asList(
              METRIC_PUBLISHERS,
              METRIC_PUBLISHERS_PUBLISHED,
              METRIC_PUBLISHERS_CONFIRMED,
              METRIC_PUBLISHERS_ERRORED,
              METRIC_CONSUMERS,
              METRIC_CONSUMERS_CONSUMED));

  static Metrics parseMetrics(String content) throws IOException {
    Metrics metrics = new Metrics();
    try (BufferedReader reader = new BufferedReader(new StringReader(content))) {
      String line;
      String type = null, name = null;
      Metric metric = null;
      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty()
            || !line.contains(METRIC_PREFIX)
            || line.contains("ct_rabbitmq_stream_prometheus")
            || line.contains("ct-rabbitmq_stream_prometheus")) {
          // empty line, non-stream metrics,
          // or line containing the name of the erlang node, which is the name of the test suite
          // the latter shows up in some metrics
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
            String l = line.substring(line.indexOf("{") + 1, line.indexOf("}"));
            labels =
                Arrays.stream(l.split(","))
                    .map(label -> label.trim().split("="))
                    .collect(
                        () -> new HashMap<>(),
                        (acc, keyValue) -> acc.put(keyValue[0], keyValue[1].replace("\"", "")),
                        (BiConsumer<Map<String, String>, Map<String, String>>)
                            (stringStringHashMap, stringStringHashMap2) ->
                                stringStringHashMap.putAll(stringStringHashMap2));
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

  static class MetricValue {

    final int value;
    final Map<String, String> labels;

    MetricValue(int value, Map<String, String> labels) {
      this.value = value;
      this.labels = labels == null ? Collections.emptyMap() : labels;
    }

    public int value() {
      return value;
    }

    @Override
    public String toString() {
      return "MetricValue{" + "value=" + value + ", labels=" + labels + '}';
    }
  }

  static class Metric {

    final String name;
    final String type;
    final String help;
    final List<MetricValue> values = new ArrayList<>();

    Metric(String name, String type, String help) {
      this.name = name.replace(METRIC_PREFIX, "");
      this.type = type;
      this.help = help;
    }

    void add(MetricValue value) {
      values.add(value);
    }

    boolean isGauge() {
      return "gauge".equals(type);
    }

    boolean isCounter() {
      return "counter".equals(type);
    }

    int value() {
      if (values.size() != 1) {
        throw new IllegalStateException();
      }
      return values.get(0).value;
    }

    public List<MetricValue> values() {
      return values;
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
      return "Metric{"
          + "name='"
          + name
          + '\''
          + ", type='"
          + type
          + '\''
          + ", help='"
          + help
          + '\''
          + ", values="
          + values
          + '}';
    }
  }

  static class Metrics {

    final Map<String, Metric> metrics = new HashMap<>();

    void add(Metric metric) {
      this.metrics.put(metric.name, metric);
    }

    Metric get(String name) {
      return metrics.get(name);
    }

    @Override
    public String toString() {
      return "Metrics{" + "metrics=" + metrics + '}';
    }
  }
}
