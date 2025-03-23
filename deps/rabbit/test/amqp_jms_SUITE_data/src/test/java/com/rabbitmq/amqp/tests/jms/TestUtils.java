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

import com.rabbitmq.qpid.protonj2.client.Client;
import com.rabbitmq.qpid.protonj2.client.ConnectionOptions;
import com.rabbitmq.qpid.protonj2.client.exceptions.ClientException;
import java.lang.annotation.*;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Hashtable;
import java.util.Map;
import java.util.UUID;
import javax.naming.Context;
import javax.naming.NamingException;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;

final class TestUtils {

  private static final String DEFAULT_BROKER_URI = "amqp://localhost:5672";

  private TestUtils() {}

  static String brokerUri() {
    String uri = System.getProperty("rmq_broker_uri", "amqp://localhost:5672");
    return uri == null || uri.isEmpty() ? DEFAULT_BROKER_URI : uri;
  }

  static String brokerHost() {
    try {
      URI uri = new URI(brokerUri());
      return uri.getHost();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  static int brokerPort() {
    try {
      URI uri = new URI(brokerUri());
      return uri.getPort();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  static String adminUsername() {
    return "guest";
  }

  static String adminPassword() {
    return "guest";
  }

  static Context context(Map<String, String> extraEnv) {
    // Configure a JNDI initial context, see
    // https://github.com/apache/qpid-jms/blob/main/qpid-jms-docs/Configuration.md#configuring-a-jndi-initialcontext
    Hashtable<Object, Object> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");

    // For a list of options, see
    // https://github.com/apache/qpid-jms/blob/main/qpid-jms-docs/Configuration.md#jms-configuration-options
    String uri =
        brokerUri()
            + "?"
            + "jms.clientID=my-client-id"
            + "&jms.username="
            + adminUsername()
            + "&jms.password="
            + adminPassword()
            + "&jms.populateJMSXUserID=true"
            + "&amqp.saslMechanisms=PLAIN";
    env.put("connectionfactory.testConnectionFactory", uri);

    env.putAll(extraEnv);

    try {
      return new javax.naming.InitialContext(env);
    } catch (NamingException e) {
      throw new RuntimeException(e);
    }
  }

  static String queueAddress(String name) {
    // no path encoding, use names with e.g. ASCII characters only
    return "/queues/" + name;
  }

  static Client protonClient() {
    return Client.create();
  }

  static com.rabbitmq.qpid.protonj2.client.Connection protonConnection(Client client) {
    ConnectionOptions connectionOptions = new ConnectionOptions().virtualHost("vhost:/");
    connectionOptions.saslOptions().addAllowedMechanism("ANONYMOUS");
    try {
      return client.connect(brokerHost(), brokerPort(), connectionOptions);
    } catch (ClientException e) {
      throw new RuntimeException(e);
    }
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

  @Target(ElementType.PARAMETER)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @interface Classic {}

  @Target(ElementType.PARAMETER)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @interface QueueArg {
    String name();

    String value();

    Class<?> type() default String.class;
  }

  @Target(ElementType.PARAMETER)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @interface QueueArgList {
    String name();

    String[] values();
  }

  @Target(ElementType.PARAMETER)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @interface QueueArgBool {
    String name();

    boolean value();
  }

  @Target(ElementType.PARAMETER)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @interface QueueArgs {
    QueueArg[] stringArgs() default {};

    QueueArgList[] listArgs() default {};

    QueueArgBool[] boolArgs() default {};
  }
}
