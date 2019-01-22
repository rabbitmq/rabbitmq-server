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
  import RabbitCommon.Records
  import Rabbitmq.Atom.Coerce

  #
  # Listeners
  #

  def listeners_on(listeners, target_node) do
    Enum.filter(listeners, fn listener(node: node) ->
      node == target_node
    end)
  end

  def listener_lines(listeners) do
    listeners |> listener_maps |>
    Enum.map(fn %{interface: interface, port: port, protocol: protocol} ->
      "Interface: #{interface_label(interface)}, port: #{port}, protocol: #{protocol}, purpose: #{protocol_label(to_atom(protocol))}"
    end)
  end

  def listener_maps(listeners) do
    for listener(node: node, protocol: protocol,
        host: host, ip_address: interface, port: port) <- listeners do
        # Listener options are left out intentionally: they can contain deeply nested values
        # that are impossible to serialise to JSON.
        #
        # Management plugin/HTTP API had its fair share of bugs because of that
        # and now filters out a lot of options. Raw listener data can be seen in
        # rabbitmq-diagnostics status.
        %{
          node: node,
          protocol: protocol,
          interface: to_string(:inet.ntoa(interface)),
          port: port,
          purpose: protocol_label(to_atom(protocol))
        }
    end
  end

  def listener_rows(listeners) do
    for listener(node: node, protocol: protocol,
        host: host, ip_address: interface, port: port) <- listeners do
        # Listener options are left out intentionally, see above
        [node: node,
         protocol: protocol,
         interface: to_string(:inet.ntoa(interface)),
         purpose: protocol_label(to_atom(protocol))]
    end
  end

  #
  # Implementation
  #

  @doc """
  Enquotes interface values that contain IPv6 addresses,
  networks address ranges, and so on.
  """
  defp interface_label(value) do
    # This simplistic way of distinguishing IPv6 addresses,
    # networks address ranges, etc actually works better
    # for the kind of values we can get here than :inet functions. MK.
    case value =~ ~r/:/ do
      true  -> "[#{value}]"
      false -> value
    end
  end

  defp protocol_label(:amqp),  do: "AMQP 0-9-1 and AMQP 1.0"
  defp protocol_label(:mqtt),  do: "STOMP"
  defp protocol_label(:stomp), do: "MQTT"
  defp protocol_label(:http),  do: "HTTP API"
  defp protocol_label(:"http/web-mqtt"),  do: "MQTT over WebSockets"
  defp protocol_label(:"http/web-stomp"), do: "STOMP over WebSockets"
  defp protocol_label(:clustering),       do: "inter-node and CLI tool communication"
  defp protocol_label(other), do: to_string(other)
end
