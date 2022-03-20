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
// Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
//

package com.rabbitmq.stream;

import static com.rabbitmq.stream.TestUtils.ResponseConditions.ko;
import static com.rabbitmq.stream.TestUtils.ResponseConditions.ok;
import static com.rabbitmq.stream.TestUtils.ResponseConditions.responseCode;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.impl.Client;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.Client.StreamMetadata;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class ClusterSizeTest {

  TestUtils.ClientFactory cf;

  @ParameterizedTest
  @ValueSource(strings = {"-1", "0"})
  void clusterSizeZeroShouldReturnError(String clusterSize) {
    Client client = cf.get(new Client.ClientParameters().port(TestUtils.streamPortNode1()));
    String s = UUID.randomUUID().toString();
    Response response =
        client.create(s, Collections.singletonMap("initial-cluster-size", clusterSize));
    assertThat(response).is(ko()).has(responseCode(Constants.RESPONSE_CODE_PRECONDITION_FAILED));
  }

  @ParameterizedTest
  @CsvSource({"1,1", "2,2", "3,3", "5,3"})
  void clusterSizeShouldReflectOnMetadata(String requestedClusterSize, int expectedClusterSize)
      throws InterruptedException {
    Client client = cf.get(new Client.ClientParameters().port(TestUtils.streamPortNode1()));
    String s = UUID.randomUUID().toString();
    try {
      Response response =
          client.create(s, Collections.singletonMap("initial-cluster-size", requestedClusterSize));
      assertThat(response).is(ok());
      StreamMetadata metadata = client.metadata(s).get(s);
      assertThat(metadata).isNotNull();
      assertThat(metadata.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
      TestUtils.waitUntil(
          () -> {
            StreamMetadata m = client.metadata(s).get(s);
            assertThat(metadata).isNotNull();
            int actualClusterSize = m.getLeader() == null ? 0 : 1 + m.getReplicas().size();
            return actualClusterSize == expectedClusterSize;
          });
    } finally {
      client.delete(s);
    }
  }
}
