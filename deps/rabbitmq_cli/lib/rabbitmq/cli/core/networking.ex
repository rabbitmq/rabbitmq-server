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
end
