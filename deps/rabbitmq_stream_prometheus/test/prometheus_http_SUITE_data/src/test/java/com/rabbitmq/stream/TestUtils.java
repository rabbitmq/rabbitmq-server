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

import static org.junit.jupiter.api.Assertions.fail;

import com.rabbitmq.stream.MetricsUtils.Metric;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.TestInfo;

public class TestUtils {

  static int streamPort() {
    String port = System.getProperty("stream.port", "5552");
    return Integer.valueOf(port);
  }

  static int prometheusPort() {
    String port = System.getProperty("prometheus.port", "15692");
    return Integer.valueOf(port);
  }

  static void waitUntil(CallableBooleanSupplier condition) throws Exception {
    waitAtMost(Duration.ofSeconds(10), condition);
  }

  static void waitAtMost(Duration duration, CallableBooleanSupplier condition) throws Exception {
    if (condition.getAsBoolean()) {
      return;
    }
    int waitTime = 100;
    int waitedTime = 0;
    long timeoutInMs = duration.toMillis();
    while (waitedTime <= timeoutInMs) {
      Thread.sleep(waitTime);
      if (condition.getAsBoolean()) {
        return;
      }
      waitedTime += waitTime;
    }
    fail("Waited " + duration.getSeconds() + " second(s), condition never got true");
  }

  @FunctionalInterface
  interface CallableBooleanSupplier {
    boolean getAsBoolean() throws Exception;
  }

  @FunctionalInterface
  interface CallableSupplier<T> {
    T get() throws Exception;
  }

  static String streamName(TestInfo info) {
    return streamName(info.getTestClass().get(), info.getTestMethod().get());
  }

  private static String streamName(Class<?> testClass, Method testMethod) {
    String uuid = UUID.randomUUID().toString();
    return String.format(
        "%s_%s%s",
        testClass.getSimpleName(), testMethod.getName(), uuid.substring(uuid.length() / 2));
  }

  static Condition<Metric> gauge() {
    return new Condition<>(m -> m.isGauge(), "should be a gauge");
  }

  static Condition<Metric> counter() {
    return new Condition<>(m -> m.isCounter(), "should be a counter");
  }

  static Condition<Metric> help() {
    return new Condition<>(m -> m.help != null, "should have a help description");
  }

  static Condition<Metric> zero() {
    return new Condition<>(
        m -> m.values.size() == 1 && m.values.get(0).value == 0, "should have one metric at 0");
  }

  static Condition<Metric> noValue() {
    return new Condition<>(m -> m.values.isEmpty(), "should have no value");
  }

  static Condition<Metric> value(int expected) {
    return new Condition<>(m -> m.value() == expected, "should have value " + expected);
  }

  static Condition<Metric> valueCount(int expected) {
    return new Condition<>(m -> m.values.size() == expected, "should have " + expected + " values");
  }

  static Condition<Metric> valuesWithLabels(String... expectedLabels) {
    Collection<String> expected = Arrays.asList(expectedLabels);
    return new Condition<>(
        m ->
            m.values().stream()
                .map(v -> v.labels.keySet())
                .map(labels -> labels.containsAll(expected))
                .reduce(true, (b1, b2) -> b1 && b2),
        "should have values with labels " + String.join(",", expected));
  }

  static Condition<Metric> value(String labelKey, String labelValue, int value) {
    return new Condition<>(
        m ->
            m.values().stream()
                    .filter(v -> v.labels.containsKey(labelKey))
                    .filter(v -> v.labels.get(labelKey).equals(labelValue))
                    .filter(v -> v.value() == value)
                    .count()
                >= 1,
        "should have value with %s=%s %d",
        labelKey,
        labelValue,
        value);
  }
}
