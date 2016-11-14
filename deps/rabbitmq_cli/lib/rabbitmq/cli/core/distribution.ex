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
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.

alias RabbitMQ.CLI.Core.Config, as: Config

defmodule RabbitMQ.CLI.Core.Distribution do

  #
  # API
  #
  def start() do
    start(%{})
  end

  def start(options) do
    node_name_type = Config.get_option(:longnames, options)
    start(node_name_type, 10, :undefined)
  end

  def start_as(node_name, opts) do
    :rabbit_nodes.ensure_epmd()
    node_name_type = Config.get_option(:longnames, opts)
    :net_kernel.start([node_name, node_name_type])
  end

  #
  # Implementation
  #

  defp start(_opt, 0, last_err) do
    {:error, last_err}
  end

  defp start(node_name_type, attempts, _last_err) do
    candidate = String.to_atom("rabbitmqcli" <>
                               to_string(:rabbit_misc.random(100)))
    case :net_kernel.start([candidate, node_name_type]) do
      {:ok, _} = ok -> ok;
      {:error, {:already_started, pid}} -> {:ok, pid};
      {:error, reason} -> start(node_name_type, attempts - 1, reason)
    end
  end
end
