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
// Copyright (c) 2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
//
package com.rabbitmq.amqp.tests.jms;


import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;
import java.lang.reflect.Field;
import org.junit.jupiter.api.extension.*;

final class JmsTestInfrastructureExtension
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(JmsTestInfrastructureExtension.class);

  private static ExtensionContext.Store store(ExtensionContext extensionContext) {
    return extensionContext.getRoot().getStore(NAMESPACE);
  }

  private static Field field(Class<?> cls, String name) {
    Field field = null;
    while (field == null && cls != null) {
      try {
        field = cls.getDeclaredField(name);
      } catch (NoSuchFieldException e) {
        cls = cls.getSuperclass();
      }
    }
    return field;
  }

  @Override
  public void beforeAll(ExtensionContext context) {

  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    Field field = field(context.getTestInstance().get().getClass(), "destination");
    if (field != null) {
      field.setAccessible(true);
      String destination = TestUtils.name(context);
      field.set(context.getTestInstance().get(), destination);
      try (Environment environment = new AmqpEnvironmentBuilder().build();
           Connection connection = environment.connectionBuilder().uri(TestUtils.brokerUri()).build()) {
        connection.management().queue(destination).declare();
      }
    }
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    Field field = field(context.getTestInstance().get().getClass(), "destination");
    if (field != null) {
      field.setAccessible(true);
      String destination = (String) field.get(context.getTestInstance().get());
      try (Environment environment = new AmqpEnvironmentBuilder().build();
           Connection connection = environment.connectionBuilder().uri(TestUtils.brokerUri()).build()) {
        connection.management().queueDelete(destination);
      }
    }
  }

  @Override
  public void afterAll(ExtensionContext context) {

  }
}
