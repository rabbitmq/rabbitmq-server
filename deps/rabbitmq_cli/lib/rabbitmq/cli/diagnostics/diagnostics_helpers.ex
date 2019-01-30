## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
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

defmodule RabbitMQ.CLI.Diagnostics.Helpers do
  import Record, only: [defrecord: 2, extract: 2]
  import RabbitCommon.Records
  import Rabbitmq.Atom.Coerce

  #
  # Listeners
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

  def check_port_connectivity(port, node_name, timeout) do
    hostname = Regex.replace(~r/^(.+)@/, to_string(node_name), "") |> to_charlist

    try do
      case :gen_tcp.connect(hostname, port, [], timeout) do
        {:error, _} ->
          false

        {:ok, port} ->
          :ok = :gen_tcp.close(port)
          true
      end

      # `gen_tcp:connect/4` will throw if the port is outside of its
      # expected domain
    catch
      :exit, _ -> false
    end
  end

  def check_listener_connectivity(%{port: port}, node_name, timeout) do
    check_port_connectivity(port, node_name, timeout)
  end

  def protocol_label(:amqp), do: "AMQP 0-9-1 and AMQP 1.0"
  def protocol_label(:mqtt), do: "MQTT"
  def protocol_label(:stomp), do: "STOMP"
  def protocol_label(:http), do: "HTTP API"
  def protocol_label(:"http/web-mqtt"), do: "MQTT over WebSockets"
  def protocol_label(:"http/web-stomp"), do: "STOMP over WebSockets"
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
      "mqtt3.1" -> "mqtt"
      "mqtt3.1.1" -> "mqtt"
      "mqtt31" -> "mqtt"
      "mqtt311" -> "mqtt"
      "mqtt3_1" -> "mqtt"
      "mqtt3_1_1" -> "mqtt"
      "stomp1.0" -> "stomp"
      "stomp1.1" -> "stomp"
      "stomp1.2" -> "stomp"
      "stomp10" -> "stomp"
      "stomp11" -> "stomp"
      "stomp12" -> "stomp"
      "stomp1_0" -> "stomp"
      "stomp1_1" -> "stomp"
      "stomp1_2" -> "stomp"
      "https" -> "http"
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
      "webstomp" -> "http/web-stomp"
      "web-stomp" -> "http/web-stomp"
      "web_stomp" -> "http/web-stomp"
      _ -> val
    end
  end

  #
  # Alarms
  #

  def alarm_lines(alarms, node_name) do
    Enum.reduce(alarms, [], fn
      :file_descriptor_limit, acc ->
        ["File descriptor limit alarm on node #{node_name}" | acc]

      {{:resource_limit, :memory, alarmed_node_name}, _}, acc ->
        ["Memory alarm on node #{alarmed_node_name}" | acc]

      {{:resource_limit, :disk, alarmed_node_name}, _}, acc ->
        ["Free disk space alarm on node #{alarmed_node_name}" | acc]
    end)
    |> Enum.reverse()
  end

  def local_alarms(alarms, node_name) do
    Enum.filter(
      alarms,
      fn
        # local by definition
        :file_descriptor_limit ->
          true

        {{:resource_limit, _, a_node}, _} ->
          node_name == a_node
      end
    )
  end

  def clusterwide_alarms(alarms, node_name) do
    alarms
    |> Enum.reject(fn x -> x == :file_descriptor_limit end)
    |> Enum.filter(fn {{:resource_limit, _, a_node}, _} ->
      a_node != node_name
    end)
  end

  #
  # Implementation
  #

  defp maybe_enquote_interface(value) do
    # This simplistic way of distinguishing IPv6 addresses,
    # networks address ranges, etc actually works better
    # for the kind of values we can get here than :inet functions. MK.
    case value =~ ~r/:/ do
      true -> "[#{value}]"
      false -> value
    end
  end
end
