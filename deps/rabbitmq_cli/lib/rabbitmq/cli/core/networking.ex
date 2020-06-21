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
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.Networking do
  @type address_family() :: :inet | :inet6

  @spec address_family(String.t() | atom() | charlist() | binary()) :: address_family()
  def address_family(value) do
    val = Rabbitmq.Atom.Coerce.to_atom(value)
    case val do
      :inet  -> :inet
      :inet4 -> :inet
      :inet6 -> :inet6
      :ipv4  -> :inet
      :ipv6  -> :inet6
      :IPv4  -> :inet
      :IPv6  -> :inet6
    end
  end

  @spec address_family(String.t() | atom()) :: boolean()
  def valid_address_family?(value) when is_atom(value) do
    valid_address_family?(to_string(value))
  end
  def valid_address_family?("inet"),  do: true
  def valid_address_family?("inet4"), do: true
  def valid_address_family?("inet6"), do: true
  def valid_address_family?("ipv4"),  do: true
  def valid_address_family?("ipv6"),  do: true
  def valid_address_family?("IPv4"),  do: true
  def valid_address_family?("IPv6"),  do: true
  def valid_address_family?(_other),  do: false

  @spec format_address(:inet.ip_address()) :: String.t()
  def format_address(addr) do
    to_string(:inet.ntoa(addr))
  end

  @spec format_addresses([:inet.ip_address()]) :: [String.t()]
  def format_addresses(addrs) do
    Enum.map(addrs, &format_address/1)
  end

  @spec inetrc_map(nonempty_list()) :: map()
  def inetrc_map(list) do
    Enum.reduce(list, %{},
      fn hosts, acc when is_list(hosts) ->
           Map.put(acc, "hosts", host_resolution_map(hosts))
         {k, v}, acc when k == :domain or k == :resolv_conf or k == :hosts_file ->
           Map.put(acc, to_string(k), to_string(v))
         {k, v}, acc when is_list(v) when k == :search or k == :lookup ->
           Map.put(acc, to_string(k), Enum.join(Enum.map(v, &to_string/1), ", "))
         {k, v}, acc when is_integer(v) ->
           Map.put(acc, to_string(k), v)
         {k, v, v2}, acc when is_tuple(v) when k == :nameserver or k == :nameservers or k == :alt_nameserver ->
           Map.put(acc, to_string(k), "#{:inet.ntoa(v)}:#{v2}")
         {k, v}, acc when is_tuple(v) when k == :nameserver or k == :nameservers or k == :alt_nameserver ->
           Map.put(acc, to_string(k), to_string(:inet.ntoa(v)))
         {k, v}, acc ->
           Map.put(acc, to_string(k), to_string(v))
      end)
  end

  def host_resolution_map(hosts) do
    Enum.reduce(hosts, %{},
        fn {:host, address, hosts}, acc ->
          Map.put(acc, to_string(:inet.ntoa(address)), Enum.map(hosts, &to_string/1))
        end)
  end
end
