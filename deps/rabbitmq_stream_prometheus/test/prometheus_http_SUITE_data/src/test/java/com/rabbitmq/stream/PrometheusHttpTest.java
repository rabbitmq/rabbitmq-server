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

import static com.rabbitmq.stream.MetricsUtils.METRICS;
import static com.rabbitmq.stream.MetricsUtils.METRIC_CONSUMERS;
import static com.rabbitmq.stream.MetricsUtils.METRIC_CONSUMERS_CONSUMED;
import static com.rabbitmq.stream.MetricsUtils.METRIC_PUBLISHERS;
import static com.rabbitmq.stream.MetricsUtils.METRIC_PUBLISHERS_CONFIRMED;
import static com.rabbitmq.stream.MetricsUtils.METRIC_PUBLISHERS_ERRORED;
import static com.rabbitmq.stream.MetricsUtils.METRIC_PUBLISHERS_PUBLISHED;
import static com.rabbitmq.stream.MetricsUtils.Metric;
import static com.rabbitmq.stream.MetricsUtils.MetricValue;
import static com.rabbitmq.stream.MetricsUtils.parseMetrics;
import static com.rabbitmq.stream.TestUtils.counter;
import static com.rabbitmq.stream.TestUtils.gauge;
import static com.rabbitmq.stream.TestUtils.help;
import static com.rabbitmq.stream.TestUtils.noValue;
import static com.rabbitmq.stream.TestUtils.value;
import static com.rabbitmq.stream.TestUtils.valueCount;
import static com.rabbitmq.stream.TestUtils.valuesWithLabels;
import static com.rabbitmq.stream.TestUtils.waitUntil;
import static com.rabbitmq.stream.TestUtils.zero;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.condition.AllOf.allOf;

import com.rabbitmq.stream.MetricsUtils.Metrics;
import com.rabbitmq.stream.TestUtils.CallableSupplier;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class PrometheusHttpTest {

  static OkHttpClient httpClient = new OkHttpClient.Builder().build();

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

  static Metrics metricsPerObject() throws IOException {
    return parseMetrics(get("/per-object"));
  }

  @ParameterizedTest
  @CsvSource({
    METRIC_PUBLISHERS + ",true",
    METRIC_PUBLISHERS_PUBLISHED + ",false",
    METRIC_PUBLISHERS_CONFIRMED + ",false",
    METRIC_PUBLISHERS_ERRORED + ",false",
    METRIC_CONSUMERS + ",true",
    METRIC_CONSUMERS_CONSUMED + ",false"
  })
  void aggregatedMetricsWithNoConnectionShouldReturnZero(String name, boolean isGauge)
      throws Exception {
    Metrics metrics = metrics();
    assertThat(metrics.metrics).hasSameSizeAs(METRICS);
    Metric metric = metrics.get(name);
    assertThat(metric).isNotNull().has(help()).is(zero()).is(isGauge ? gauge() : counter());
  }

  @Test
  void perObjectMetricsWithNoConnectionShouldReturnNoValue() throws Exception {
    Metrics metrics = metricsPerObject();
    METRICS.forEach(
        name -> {
          Metric metric = metrics.get(name);
          if (METRIC_PUBLISHERS.equals(name) || METRIC_CONSUMERS.equals(name)) {
            assertThat(metric).isNull();
          } else {
            assertThat(metric).isNotNull().has(noValue());
          }
        });
  }

  @Test
  void aggregatedMetricsWithPublishersAndConsumersShouldReturnCorrectCounts(TestInfo info)
      throws Exception {
    List<String> streams =
        IntStream.range(0, 5).mapToObj(i -> TestUtils.streamName(info)).collect(toList());
    int producersCount = streams.size();
    int consumersCount = streams.size() * 2;
    int messagesByProducer = 10_000;
    int messageCount = producersCount * messagesByProducer;

    Environment env = Environment.builder().port(TestUtils.streamPort()).build();
    List<Producer> producers = Collections.emptyList();
    List<Consumer> consumers = Collections.emptyList();
    CallableSupplier<Metrics> metricsCall = () -> metrics();
    try {
      streams.forEach(stream -> env.streamCreator().stream(stream).create());

      producers =
          IntStream.range(0, producersCount)
              .mapToObj(i -> env.producerBuilder().stream(streams.get(i % streams.size())).build())
              .collect(toList());

      waitUntil(() -> metricsCall.get().get(METRIC_PUBLISHERS).value() == producersCount);

      CountDownLatch confirmedLatch = new CountDownLatch(messageCount);
      ConfirmationHandler confirmationHandler = status -> confirmedLatch.countDown();
      producers.forEach(
          producer -> {
            IntStream.range(0, messagesByProducer)
                .forEach(
                    i ->
                        producer.send(
                            producer.messageBuilder().addData("".getBytes()).build(),
                            confirmationHandler));
          });

      assertThat(confirmedLatch.await(10, TimeUnit.SECONDS)).isTrue();

      waitUntil(() -> metricsCall.get().get(METRIC_PUBLISHERS_CONFIRMED).value() == messageCount);

      Metrics metrics = metricsCall.get();
      assertThat(metrics.get(METRIC_PUBLISHERS_PUBLISHED)).has(value(messageCount));
      assertThat(metrics.get(METRIC_PUBLISHERS_CONFIRMED)).has(value(messageCount));
      assertThat(metrics.get(METRIC_PUBLISHERS_ERRORED)).is(zero());
      assertThat(metrics.get(METRIC_CONSUMERS)).is(zero());
      assertThat(metrics.get(METRIC_CONSUMERS_CONSUMED)).is(zero());

      int consumedMessageCount = consumersCount * messagesByProducer;
      CountDownLatch consumedLatch = new CountDownLatch(consumedMessageCount);
      consumers =
          IntStream.range(0, consumersCount)
              .mapToObj(
                  i ->
                      env.consumerBuilder().stream(streams.get(i % streams.size()))
                          .offset(OffsetSpecification.first())
                          .messageHandler((ctx, msg) -> consumedLatch.countDown())
                          .build())
              .collect(toList());

      assertThat(consumedLatch.await(10, TimeUnit.SECONDS)).isTrue();

      waitUntil(
          () -> metricsCall.get().get(METRIC_CONSUMERS_CONSUMED).value() == consumedMessageCount);

      metrics = metricsCall.get();
      assertThat(metrics.get(METRIC_CONSUMERS)).has(value(consumersCount));
      assertThat(metrics.get(METRIC_CONSUMERS_CONSUMED)).has(value(consumedMessageCount));

    } finally {
      producers.forEach(producer -> producer.close());
      consumers.forEach(consumer -> consumer.close());
      streams.forEach(stream -> env.deleteStream(stream));
      env.close();
    }
  }

  @Test
  void perObjectMetricsWithPublishersAndConsumersShouldReturnCorrectCounts(TestInfo info)
      throws Exception {
    List<String> streams =
        IntStream.range(0, 5).mapToObj(i -> TestUtils.streamName(info)).collect(toList());
    int producersCount = streams.size();
    int consumersCount = streams.size() * 2;
    int messagesByProducer = 10_000;
    int messageCount = producersCount * messagesByProducer;

    Environment env = Environment.builder().port(TestUtils.streamPort()).build();
    List<Producer> producers = Collections.emptyList();
    List<Consumer> consumers = Collections.emptyList();
    CallableSupplier<Metrics> metricsCall = () -> metricsPerObject();
    try {
      streams.forEach(stream -> env.streamCreator().stream(stream).create());

      producers =
          IntStream.range(0, producersCount)
              .mapToObj(i -> env.producerBuilder().stream(streams.get(i % streams.size())).build())
              .collect(toList());

      CountDownLatch confirmedLatch = new CountDownLatch(messageCount);
      ConfirmationHandler confirmationHandler = status -> confirmedLatch.countDown();
      producers.forEach(
          producer -> {
            IntStream.range(0, messagesByProducer)
                .forEach(
                    i ->
                        producer.send(
                            producer.messageBuilder().addData("".getBytes()).build(),
                            confirmationHandler));
          });

      assertThat(confirmedLatch.await(10, TimeUnit.SECONDS)).isTrue();

      waitUntil(
          () ->
              metricsCall.get().get(METRIC_PUBLISHERS_CONFIRMED).values().stream()
                      .mapToInt(MetricValue::value)
                      .sum()
                  == messageCount);

      Metrics metrics = metricsCall.get();
      assertThat(metrics.get(METRIC_PUBLISHERS)).isNull(); // no counters in per-object
      assertThat(metrics.get(METRIC_PUBLISHERS_PUBLISHED))
          .has(valueCount(producersCount))
          .has(valuesWithLabels("vhost", "queue", "connection", "id"))
          .has(
              allOf(
                  streams.stream()
                      .map(s -> value("queue", s, messagesByProducer))
                      .collect(toList())));
      assertThat(metrics.get(METRIC_PUBLISHERS_CONFIRMED))
          .has(valueCount(producersCount))
          .has(valuesWithLabels("vhost", "queue", "connection", "id"))
          .has(
              allOf(
                  streams.stream()
                      .map(s -> value("queue", s, messagesByProducer))
                      .collect(toList())));
      assertThat(metrics.get(METRIC_PUBLISHERS_ERRORED))
          .has(valueCount(producersCount))
          .has(valuesWithLabels("vhost", "queue", "connection", "id"))
          .has(allOf(streams.stream().map(s -> value("queue", s, 0)).collect(toList())));
      assertThat(metrics.get(METRIC_CONSUMERS)).isNull(); // no counters in per-object
      assertThat(metrics.get(METRIC_CONSUMERS_CONSUMED)).has(noValue());

      int consumedMessageCount = consumersCount * messagesByProducer;
      CountDownLatch consumedLatch = new CountDownLatch(consumedMessageCount);
      consumers =
          IntStream.range(0, consumersCount)
              .mapToObj(
                  i ->
                      env.consumerBuilder().stream(streams.get(i % streams.size()))
                          .offset(OffsetSpecification.first())
                          .messageHandler((ctx, msg) -> consumedLatch.countDown())
                          .build())
              .collect(toList());

      assertThat(consumedLatch.await(10, TimeUnit.SECONDS)).isTrue();

      waitUntil(
          () ->
              metricsCall.get().get(METRIC_CONSUMERS_CONSUMED).values().stream()
                      .mapToInt(MetricValue::value)
                      .sum()
                  == consumedMessageCount);

      metrics = metricsCall.get();
      assertThat(metrics.get(METRIC_CONSUMERS)).isNull(); // no counters in per-object
      assertThat(metrics.get(METRIC_CONSUMERS_CONSUMED))
          .has(valueCount(consumersCount))
          .has(valuesWithLabels("vhost", "queue", "connection", "id"))
          .has(
              allOf(
                  streams.stream()
                      .flatMap(s -> Stream.of(s, s))
                      .map(s -> value("queue", s, messagesByProducer))
                      .collect(toList())));

    } finally {
      producers.forEach(producer -> producer.close());
      consumers.forEach(consumer -> consumer.close());
      streams.forEach(stream -> env.deleteStream(stream));
      env.close();
    }
  }
}
