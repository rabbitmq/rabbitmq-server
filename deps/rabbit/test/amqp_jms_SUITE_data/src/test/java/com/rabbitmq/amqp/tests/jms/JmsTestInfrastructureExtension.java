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

import static java.util.Collections.singletonMap;

import com.rabbitmq.amqp.tests.jms.TestUtils.*;
import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.Management;
import com.rabbitmq.client.amqp.Management.QueueSpecification;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Queue;
import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Predicate;
import javax.naming.Context;
import javax.naming.NamingException;
import org.junit.jupiter.api.extension.*;

final class JmsTestInfrastructureExtension
    implements BeforeEachCallback, AfterEachCallback, ParameterResolver {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(JmsTestInfrastructureExtension.class);

  private static ExtensionContext.Store store(ExtensionContext extensionContext) {
    return extensionContext.getRoot().getStore(NAMESPACE);
  }

  private static Optional<Field> field(Class<?> cls, Predicate<Field> predicate) {
    for (Field field : cls.getDeclaredFields()) {
      if (predicate.test(field)) {
        return Optional.of(field);
      }
    }
    return Optional.empty();
  }

  private static boolean isQueue(Parameter parameter) {
    return Queue.class.isAssignableFrom(parameter.getType());
  }

  private static Management.QueueType queueType(Parameter parameter) {
    return parameter.isAnnotationPresent(Classic.class)
        ? Management.QueueType.CLASSIC
        : Management.QueueType.QUORUM;
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    if (context.getTestMethod().isPresent()) {
      String queueName;
      for (Parameter parameter : context.getTestMethod().get().getParameters()) {
        if (isQueue(parameter)) {
          queueName = TestUtils.name(context);
          String queueAddress = TestUtils.queueAddress(queueName);
          try (Environment environment = new AmqpEnvironmentBuilder().build();
              Connection connection =
                  environment.connectionBuilder().uri(TestUtils.brokerUri()).build()) {

            Management.QueueType type = queueType(parameter);
            QueueSpecification queueSpec = connection.management().queue(queueName).type(type);

            if (parameter.isAnnotationPresent(QueueArgs.class)) {
              QueueArgs queueArgs = parameter.getAnnotation(QueueArgs.class);

              for (QueueArg arg : queueArgs.stringArgs()) {
                if (arg.type() == Boolean.class) {
                  queueSpec = queueSpec.argument(arg.name(), Boolean.parseBoolean(arg.value()));
                } else {
                  queueSpec = queueSpec.argument(arg.name(), arg.value());
                }
              }
              for (QueueArgBool arg : queueArgs.boolArgs()) {
                queueSpec = queueSpec.argument(arg.name(), arg.value());
              }
              for (QueueArgList arg : queueArgs.listArgs()) {
                queueSpec = queueSpec.argument(arg.name(), arg.values());
              }
            }

            queueSpec.declare();
          }
          store(context).put("queueName", queueName);
          Context jndiContext = TestUtils.context(singletonMap("queue." + queueName, queueAddress));
          store(context).put("jndiContext", jndiContext);
        }
      }

      if (context.getTestInstance().isPresent()) {
        Optional<Field> connectionFactoryField =
            field(
                context.getTestInstance().get().getClass(),
                field -> ConnectionFactory.class.isAssignableFrom(field.getType()));
        if (connectionFactoryField.isPresent()) {
          connectionFactoryField.get().setAccessible(true);
          Context jndiContext =
              store(context)
                  .getOrComputeIfAbsent(
                      "jndiContext", k -> TestUtils.context(Collections.emptyMap()), Context.class);
          ConnectionFactory connectionFactory =
              (ConnectionFactory) jndiContext.lookup("testConnectionFactory");
          connectionFactoryField.get().set(context.getTestInstance().get(), connectionFactory);
        }
      }
    }
  }

  @Override
  public void afterEach(ExtensionContext context) {
    String queueName = store(context).remove("queueName", String.class);
    if (queueName != null) {
      try (Environment environment = new AmqpEnvironmentBuilder().build();
          Connection connection =
              environment.connectionBuilder().uri(TestUtils.brokerUri()).build()) {
        connection.management().queueDelete(queueName);
      }
    }
    store(context).remove("jndiContext", Context.class);
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return isQueue(parameterContext.getParameter());
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    String queueName = store(extensionContext).get("queueName", String.class);
    Context jndiContext = store(extensionContext).get("jndiContext", Context.class);
    try {
      return jndiContext.lookup(queueName);
    } catch (NamingException e) {
      throw new RuntimeException(e);
    }
  }
}
