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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.extension.*;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class TestUtils {

    static int streamPortNode1() {
        String port = System.getProperty("node1.stream.port", "5555");
        return Integer.valueOf(port);
    }

    static int streamPortNode2() {
        String port = System.getProperty("node2.stream.port", "5555");
        return Integer.valueOf(port);
    }

    static void waitAtMost(Duration duration, BooleanSupplier condition) throws InterruptedException {
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

    static class StreamTestInfrastructureExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

        private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(StreamTestInfrastructureExtension.class);

        private static ExtensionContext.Store store(ExtensionContext extensionContext) {
            return extensionContext.getRoot().getStore(NAMESPACE);
        }

        private static EventLoopGroup eventLoopGroup(ExtensionContext context) {
            return (EventLoopGroup) store(context).get("nettyEventLoopGroup");
        }

        @Override
        public void beforeAll(ExtensionContext context) {
            store(context).put("nettyEventLoopGroup", new NioEventLoopGroup());
        }

        @Override
        public void beforeEach(ExtensionContext context) throws Exception {
            try {
                Field streamField = context.getTestInstance().get().getClass().getDeclaredField("eventLoopGroup");
                streamField.setAccessible(true);
                streamField.set(context.getTestInstance().get(), eventLoopGroup(context));
            } catch (NoSuchFieldException e) {

            }
            try {
                Field streamField = context.getTestInstance().get().getClass().getDeclaredField("stream");
                streamField.setAccessible(true);
                String stream = UUID.randomUUID().toString();
                streamField.set(context.getTestInstance().get(), stream);
                Client client = new Client(new Client.ClientParameters().eventLoopGroup(eventLoopGroup(context))
                        .port(streamPortNode1())
                );
                Client.Response response = client.create(stream);
                assertThat(response.isOk()).isTrue();
                client.close();
                store(context).put("testMethodStream", stream);
            } catch (NoSuchFieldException e) {

            }

            for (Field declaredField : context.getTestInstance().get().getClass().getDeclaredFields()) {
                if (declaredField.getType().equals(ClientFactory.class)) {
                    declaredField.setAccessible(true);
                    ClientFactory clientFactory = new ClientFactory(eventLoopGroup(context));
                    declaredField.set(context.getTestInstance().get(), clientFactory);
                    store(context).put("testClientFactory", clientFactory);
                    break;
                }
            }

        }

        @Override
        public void afterEach(ExtensionContext context) throws Exception {
            try {
                Field streamField = context.getTestInstance().get().getClass().getDeclaredField("stream");
                streamField.setAccessible(true);
                String stream = (String) streamField.get(context.getTestInstance().get());
                Client client = new Client(new Client.ClientParameters().eventLoopGroup(eventLoopGroup(context))
                        .port(streamPortNode1())
                );
                Client.Response response = client.delete(stream);
                assertThat(response.isOk()).isTrue();
                client.close();
                store(context).remove("testMethodStream");
            } catch (NoSuchFieldException e) {

            }

            ClientFactory clientFactory = (ClientFactory) store(context).get("testClientFactory");
            if (clientFactory != null) {
                clientFactory.close();
            }
        }

        @Override
        public void afterAll(ExtensionContext context) throws Exception {
            EventLoopGroup eventLoopGroup = eventLoopGroup(context);
            eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
        }

    }

    static class ClientFactory {

        private final EventLoopGroup eventLoopGroup;
        private final Set<Client> clients = ConcurrentHashMap.newKeySet();


        public ClientFactory(EventLoopGroup eventLoopGroup) {
            this.eventLoopGroup = eventLoopGroup;
        }

        public Client get() {
            return get(new Client.ClientParameters());
        }

        public Client get(Client.ClientParameters parameters) {
            // don't set the port, it would override the caller's port setting
            Client client = new Client(parameters.eventLoopGroup(eventLoopGroup));
            clients.add(client);
            return client;
        }

        private void close() {
            for (Client c : clients) {
                c.close();
            }
        }
    }

}
