## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Core.Listeners do
  import Record, only: [defrecord: 2, extract: 2]
  import RabbitCommon.Records
  import Rabbitmq.Atom.Coerce

  #
  # API
  #

  defrecord :hostent, extract(:hostent, from_lib: "kernel/include/inet.hrl")

  def listeners_on(listeners, target_node) do
    Enum.filter(listeners, fn listener(node: node) ->
      node == target_node
    end)
  end

  def listener_lines(listeners) do
    listeners
    |> listener_maps
    |> Enum.map(fn %{interface: interface, port: port, protocol: protocol} ->
      "Interface: #{interface}, port: #{port}, protocol: #{protocol}, purpose: #{
        protocol_label(to_atom(protocol))
      }"
    end)
  end

  def listener_map(listener) when is_map(listener) do
    listener
  end
  def listener_map(listener) do
    # Listener options are left out intentionally: they can contain deeply nested values
    # that are impossible to serialise to JSON.
    #
    # Management plugin/HTTP API had its fair share of bugs because of that
    # and now filters out a lot of options. Raw listener data can be seen in
    # rabbitmq-diagnostics status.
    listener(node: node, protocol: protocol, ip_address: interface, port: port) = listener

    %{
      node: node,
      protocol: protocol,
      interface: :inet.ntoa(interface) |> to_string |> maybe_enquote_interface,
      port: port,
      purpose: protocol_label(to_atom(protocol))
    }
  end

  def listener_maps(listeners) do
    Enum.map(listeners, &listener_map/1)
  end

  def listener_rows(listeners) do
    for listener(node: node, protocol: protocol, ip_address: interface, port: port) <- listeners do
      # Listener options are left out intentionally, see above
      [
        node: node,
        protocol: protocol,
        interface: :inet.ntoa(interface) |> to_string |> maybe_enquote_interface,
        port: port,
        purpose: protocol_label(to_atom(protocol))
      ]
    end
  end

  def protocol_label(:amqp),       do: "AMQP 0-9-1 and AMQP 1.0"
  def protocol_label(:'amqp/ssl'), do: "AMQP 0-9-1 and AMQP 1.0 over TLS"
  def protocol_label(:mqtt),       do: "MQTT"
  def protocol_label(:'mqtt/ssl'), do: "MQTT over TLS"
  def protocol_label(:stomp),       do: "STOMP"
  def protocol_label(:'stomp/ssl'), do: "STOMP over TLS"
  def protocol_label(:http),  do: "HTTP API"
  def protocol_label(:https), do: "HTTP API over TLS (HTTPS)"
  def protocol_label(:"http/web-mqtt"), do: "MQTT over WebSockets"
  def protocol_label(:"https/web-mqtt"), do: "MQTT over WebSockets and TLS (HTTPS)"
  def protocol_label(:"http/web-stomp"), do: "STOMP over WebSockets"
  def protocol_label(:"https/web-stomp"), do: "STOMP over WebSockets and TLS (HTTPS)"
  def protocol_label(:clustering), do: "inter-node and CLI tool communication"
  def protocol_label(other), do: to_string(other)

  def normalize_protocol(proto) do
    val = proto |> to_string |> String.downcase()

    case val do
      "amqp091" -> "amqp"
      "amqp0.9.1" -> "amqp"
      "amqp0-9-1" -> "amqp"
      "amqp0_9_1" -> "amqp"
      "amqp10" -> "amqp"
      "amqp1.0" -> "amqp"
      "amqp1-0" -> "amqp"
      "amqp1_0" -> "amqp"
      "amqps" -> "amqp/ssl"
      "mqtt3.1" -> "mqtt"
      "mqtt3.1.1" -> "mqtt"
      "mqtt31" -> "mqtt"
      "mqtt311" -> "mqtt"
      "mqtt3_1" -> "mqtt"
      "mqtt3_1_1" -> "mqtt"
      "mqtts"     -> "mqtt/ssl"
      "mqtt+tls"  -> "mqtt/ssl"
      "mqtt+ssl"  -> "mqtt/ssl"
      "stomp1.0" -> "stomp"
      "stomp1.1" -> "stomp"
      "stomp1.2" -> "stomp"
      "stomp10" -> "stomp"
      "stomp11" -> "stomp"
      "stomp12" -> "stomp"
      "stomp1_0" -> "stomp"
      "stomp1_1" -> "stomp"
      "stomp1_2" -> "stomp"
      "stomps"    -> "stomp/ssl"
      "stomp+tls" -> "stomp/ssl"
      "stomp+ssl" -> "stomp/ssl"
      "https" -> "https"
      "http1" -> "http"
      "http1.1" -> "http"
      "http_api" -> "http"
      "management" -> "http"
      "management_ui" -> "http"
      "ui" -> "http"
      "cli" -> "clustering"
      "distribution" -> "clustering"
      "webmqtt" -> "http/web-mqtt"
      "web-mqtt" -> "http/web-mqtt"
      "web_mqtt" -> "http/web-mqtt"
      "webmqtt/tls" -> "https/web-mqtt"
      "web-mqtt/tls" -> "https/web-mqtt"
      "webmqtt/ssl" -> "https/web-mqtt"
      "web-mqtt/ssl" -> "https/web-mqtt"
      "webmqtt+tls" -> "https/web-mqtt"
      "web-mqtt+tls" -> "https/web-mqtt"
      "webmqtt+ssl" -> "https/web-mqtt"
      "web-mqtt+ssl" -> "https/web-mqtt"
      "webstomp" -> "http/web-stomp"
      "web-stomp" -> "http/web-stomp"
      "web_stomp" -> "http/web-stomp"
      "webstomp/tls" -> "https/web-stomp"
      "web-stomp/tls" -> "https/web-stomp"
      "webstomp/ssl" -> "https/web-stomp"
      "web-stomp/ssl" -> "https/web-stomp"
      "webstomp+tls" -> "https/web-stomp"
      "web-stomp+tls" -> "https/web-stomp"
      "webstomp+ssl" -> "https/web-stomp"
      "web-stomp+ssl" -> "https/web-stomp"
      _ -> val
    end
  end

  #
  # Implementation
  #

  defp maybe_enquote_interface(value) do
    # This simplistic way of distinguishing IPv6 addresses,
    # networks address ranges, etc actually works better
    # for the kind of values we can get here than :inet functions. MK.
    regex = Regex.recompile!(~r/:/)
    case value =~ regex do
      true -> "[#{value}]"
      false -> value
    end
  end
end
