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

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import java.util.Arrays;
import org.assertj.core.api.AbstractObjectAssert;

abstract class Assertions {

  private Assertions() {}

  static JmsMessageAssert assertThat(Message message) {
    return new JmsMessageAssert(message);
  }

  static class JmsMessageAssert extends AbstractObjectAssert<JmsMessageAssert, Message> {

    private static final String JMS_X_DELIVERY_COUNT = "JMSXDeliveryCount";

    public JmsMessageAssert(Message message) {
      super(message, JmsMessageAssert.class);
    }

    public static JmsMessageAssert assertThat(Message message) {
      return new JmsMessageAssert(message);
    }

    public JmsMessageAssert isRedelivered() throws JMSException {
      isNotNull();
      if (!actual.getJMSRedelivered()) {
        failWithMessage("Message is expected to be redelivered");
      }
      return this;
    }

    public JmsMessageAssert isNotRedelivered() throws JMSException {
      isNotNull();
      if (actual.getJMSRedelivered()) {
        failWithMessage("Message is not expected to be redelivered");
      }
      return this;
    }

    public JmsMessageAssert hasDeliveryCount(int expectedDeliveryCount) throws JMSException {
      isNotNull();
      int actualDeliveryCount = this.actual.getIntProperty(JMS_X_DELIVERY_COUNT);
      if (actualDeliveryCount != expectedDeliveryCount) {
        failWithMessage(
            "Delivery count is expected to be %d but is %d",
            expectedDeliveryCount, actualDeliveryCount);
      }
      return this;
    }

    public JmsMessageAssert isTextMessage() {
      isNotNull();
      if (!(this.actual instanceof TextMessage)) {
        failWithMessage("Expected a text message, but is %s", this.actual.getClass().getName());
      }
      return this;
    }

    public JmsMessageAssert hasText(String expected) throws JMSException {
      isNotNull();
      isTextMessage();
      if (!expected.equals(this.actual.getBody(String.class))) {
        failWithMessage("Expected %s but got %s", expected, this.actual.getBody(String.class));
      }
      return this;
    }

    public JmsMessageAssert hasType(String expected) throws JMSException {
      isNotNull();
      if (!expected.equals(this.actual.getJMSType())) {
        failWithMessage("Expected %s JMSType but got %s", expected, this.actual.getJMSType());
      }
      return this;
    }

    public JmsMessageAssert hasId(String expected) throws JMSException {
      isNotNull();
      if (!expected.equals(this.actual.getJMSMessageID())) {
        failWithMessage(
            "Expected %s JMSMessageID but got %s", expected, this.actual.getJMSMessageID());
      }
      return this;
    }

    public JmsMessageAssert hasCorrelationId(String expected) throws JMSException {
      isNotNull();
      if (!expected.equals(this.actual.getJMSCorrelationID())) {
        failWithMessage(
            "Expected %s JMSCorrelationID but got %s", expected, this.actual.getJMSCorrelationID());
      }
      return this;
    }

    public JmsMessageAssert hasCorrelationId(byte[] expected) throws JMSException {
      isNotNull();
      if (!Arrays.equals(expected, this.actual.getJMSCorrelationIDAsBytes())) {
        failWithMessage(
            "Expected %s JMSCorrelationID but got %s",
            expected, this.actual.getJMSCorrelationIDAsBytes());
      }
      return this;
    }

    public JmsMessageAssert hasPriority(int expected) throws JMSException {
      isNotNull();
      if (expected != this.actual.getJMSPriority()) {
        failWithMessage(
            "Expected %d JMSPriority but got %d", expected, this.actual.getJMSPriority());
      }
      return this;
    }

    public JmsMessageAssert hasTimestamp(long expected) throws JMSException {
      isNotNull();
      if (expected != this.actual.getJMSTimestamp()) {
        failWithMessage(
            "Expected %d JMSTimestamp but got %d", expected, this.actual.getJMSTimestamp());
      }
      return this;
    }

    public JmsMessageAssert hasProperty(String key, Object expectedValue) throws JMSException {
      isNotNull();
      Object value = actual.getObjectProperty(key);
      if (value == null) {
        failWithMessage("Expected %s property but is not present", key);
      }
      if (!expectedValue.equals(value)) {
        failWithMessage("Expected %s for property %s value but got %s", expectedValue, key, value);
      }
      return this;
    }
  }
}
