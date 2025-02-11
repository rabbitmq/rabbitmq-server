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
// Copyright (c) 2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc.
// and/or its subsidiaries. All rights reserved.
//

package com.rabbitmq.amqp.tests.jms;

import static java.lang.String.format;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import java.lang.reflect.Method;
import java.util.UUID;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsQueue;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;

final class TestUtils {

  private static final String DEFAULT_BROKER_URI = "amqp://localhost:5672";

  private TestUtils() {}

  static String brokerUri() {
    String uri = System.getProperty("rmq_broker_uri", "amqp://localhost:5672");
    return uri == null || uri.isEmpty() ? DEFAULT_BROKER_URI : uri;
  }

  static String adminUsername() {
    return "guest";
  }

  static String adminPassword() {
    return "guest";
  }

  static ConnectionFactory connectionFactory() {
    return new JmsConnectionFactory(brokerUri());
  }

  static Connection connection() throws JMSException {
    return connectionFactory().createConnection();
  }

  static Queue queue(String name) {
    // no path encoding, use names with e.g. ASCII characters only
    return new JmsQueue("/queues/" + name);
  }

  static String name(TestInfo info) {
    return name(info.getTestClass().get(), info.getTestMethod().get());
  }

  static String name(ExtensionContext context) {
    return name(context.getTestInstance().get().getClass(), context.getTestMethod().get());
  }

  private static String name(Class<?> testClass, Method testMethod) {
    return name(testClass, testMethod.getName());
  }

  private static String name(Class<?> testClass, String testMethod) {
    String uuid = UUID.randomUUID().toString();
    return format(
        "%s_%s%s", testClass.getSimpleName(), testMethod, uuid.substring(uuid.length() / 2));
  }
}
