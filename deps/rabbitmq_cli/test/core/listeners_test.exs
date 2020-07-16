## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule CoreListenersTest do
  use ExUnit.Case, async: true

  import RabbitMQ.CLI.Core.Listeners
  import RabbitCommon.Records

  test "listener record translation to a map" do
    assert listener_map(listener(node: :rabbit@mercurio,
                        protocol: :stomp,
                        ip_address: {0,0,0,0,0,0,0,0},
                        port: 61613)) ==
      %{
        interface: "[::]",
        node: :rabbit@mercurio,
        port: 61613,
        protocol: :stomp,
        purpose: "STOMP"
      }
  end

  test "[human-readable] protocol labels" do
    assert protocol_label(:amqp) == "AMQP 0-9-1 and AMQP 1.0"
    assert protocol_label(:'amqp/ssl') == "AMQP 0-9-1 and AMQP 1.0 over TLS"
    assert protocol_label(:mqtt) == "MQTT"
    assert protocol_label(:'mqtt/ssl') == "MQTT over TLS"
    assert protocol_label(:stomp) == "STOMP"
    assert protocol_label(:'stomp/ssl') == "STOMP over TLS"
    assert protocol_label(:http) == "HTTP API"
    assert protocol_label(:https) == "HTTP API over TLS (HTTPS)"
    assert protocol_label(:'https/web-stomp') == "STOMP over WebSockets and TLS (HTTPS)"
    assert protocol_label(:'https/web-mqtt') == "MQTT over WebSockets and TLS (HTTPS)"

    assert protocol_label(:'http/prometheus') == "Prometheus exporter API over HTTP"
    assert protocol_label(:'https/prometheus') == "Prometheus exporter API over TLS (HTTPS)"
  end

  test "listener expiring within" do
    validityInDays = 10
    validity = X509.Certificate.Validity.days_from_now(validityInDays)
    ca_key = X509.PrivateKey.new_ec(:secp256r1)
    ca = X509.Certificate.self_signed(ca_key,
      "/C=US/ST=CA/L=San Francisco/O=Megacorp/CN=Megacorp Intermediate CA",
      template: :root_ca,
      validity: validity
    )
    pem = X509.Certificate.to_pem(ca)

    opts = [{:certfile, {:pem, pem}}, {:cacertfile, {:pem, pem}}]
    listener = listener(node: :rabbit@mercurio,
      protocol: :stomp,
      ip_address: {0,0,0,0,0,0,0,0},
      port: 61613,
      opts: opts)

    assert not listener_expiring_within(listener, 86400 * (validityInDays - 5))
    assert listener_expiring_within(listener, 86400 * (validityInDays + 5))
  end
end
