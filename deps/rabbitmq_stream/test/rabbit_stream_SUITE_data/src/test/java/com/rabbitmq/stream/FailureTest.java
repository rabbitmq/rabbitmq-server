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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class FailureTest {

    TestUtils.ClientFactory cf;
    String stream;

    @Test
    void publisherConnectedToStreamReplicaShouldBeNotified() throws Exception {
        Client client = cf.get(new Client.ClientParameters().port(TestUtils.streamPortNode1()));
        Map<String, Client.StreamMetadata> metadata = client.metadata(stream);
        Client.StreamMetadata streamMetadata = metadata.get(stream);
        assertThat(streamMetadata).isNotNull();

        assertThat(streamMetadata.getLeader().getPort()).isEqualTo(TestUtils.streamPortNode1());
        assertThat(streamMetadata.getReplicas()).isNotEmpty();
        Client.Broker replica = streamMetadata.getReplicas().get(0);
        assertThat(replica.getPort()).isNotEqualTo(TestUtils.streamPortNode1());

        AtomicReference<CountDownLatch> confirmLatch = new AtomicReference<>(new CountDownLatch(1));
        Client publisher = cf.get(new Client.ClientParameters()
                .port(replica.getPort())
                .confirmListener(publishingId -> confirmLatch.get().countDown()));
        publisher.publish(stream, "all nodes available".getBytes(StandardCharsets.UTF_8));
        assertThat(confirmLatch.get().await(10, TimeUnit.SECONDS)).isTrue();
        confirmLatch.set(null);

        Host.rabbitmqctl("stop_app");
        try {
            cf.get(new Client.ClientParameters().port(TestUtils.streamPortNode1()));
            fail("Node app stopped, connecting should not be possible");
        } catch (Exception e) {
            // OK
        }

        // wait until there's a new leader
//        TestUtils.waitAtMost(Duration.ofSeconds(5), () -> {
//            Client.StreamMetadata m = publisher.metadata(stream).get(stream);
//            System.out.println(m);
//            return m.getLeader() != null && m.getLeader().getPort() != TestUtils.streamPortNode1();
//        });
//
//        confirmLatch.set(new CountDownLatch(1));
//        publisher.publish(stream, "2 nodes available".getBytes(StandardCharsets.UTF_8));
//        assertThat(confirmLatch.get().await(10, TimeUnit.SECONDS)).isTrue();
//        confirmLatch.set(null);

        Host.rabbitmqctl("start_app");

        // wait until all the replicas are there
        TestUtils.waitAtMost(Duration.ofSeconds(5), () -> {
            Client.StreamMetadata m = publisher.metadata(stream).get(stream);
            // FIXME don't deduplicate the replicas once recovery is fixed
            Set<Client.Broker> replicas = new HashSet<>(m.getReplicas());
            return replicas.size() == 2;
        });

        confirmLatch.set(new CountDownLatch(1));
        publisher.publish(stream, "all nodes are back".getBytes(StandardCharsets.UTF_8));
        assertThat(confirmLatch.get().await(10, TimeUnit.SECONDS)).isTrue();
        confirmLatch.set(null);

        CountDownLatch consumeLatch = new CountDownLatch(2);
        Set<String> bodies = ConcurrentHashMap.newKeySet();
        Client consumer = cf.get(new Client.ClientParameters()
                .port(TestUtils.streamPortNode1())
                .messageListener((subscriptionId, offset, message) -> {
                    bodies.add(new String(message.getBodyAsBinary(), StandardCharsets.UTF_8));
                    consumeLatch.countDown();
                }));

        Client.Response response = consumer.subscribe(1, stream, OffsetSpecification.first(), 10);
        assertThat(response.isOk()).isTrue();
        assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(bodies).hasSize(2).contains("all nodes available", "all nodes are back");
    }

}
