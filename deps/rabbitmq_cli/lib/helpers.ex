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
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


# Small helper functions, mostly related to connecting to RabbitMQ.

defmodule Helpers do
  @rabbit_host "rabbit"

  # Executes generate_module_map/0 as a macro at compile time. Any
  # modules added after compilation will not show up in the map.
  def commands() do
    quote do unquote(CommandModules.generate_module_map) end
  end

  def is_command?([]), do: false
  def is_command?([head | _]), do: is_command?(head)
  def is_command?(str), do: commands[str] != nil

  def get_rabbit_hostname(), do: (@rabbit_host <> "@" <> hostname) |> String.to_atom

  # Although it is public, this method does not have any associated tests
  # because the only functionality not covered by a library is comes from
  # get_rabbit_hostname, which is itself already tested.
  def parse_node(nil), do: get_rabbit_hostname
  def parse_node(host) when is_atom(host), do: host
  def parse_node(host) when is_binary(host), do: host |> String.to_atom

  def connect_to_rabbitmq(), do:      :net_kernel.connect_node(get_rabbit_hostname)
  def connect_to_rabbitmq(input), do: :net_kernel.connect_node(input)

  defp hostname(), do: :inet.gethostname() |> elem(1) |> List.to_string
end
