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

defmodule RabbitMQ.CLI.Distribution do

  #
  # API
  #

  def start() do
    start(%{})
  end

  def start(options) do
    names_opt = case options[:longnames] do
      true  -> [:longnames];
      false -> [:shortnames];
      nil   -> [:shortnames]
    end
    start(names_opt, 10, :undefined)
  end

  def start_as(node_name) do
    :rabbit_nodes.ensure_epmd()
    :net_kernel.start([node_name, name_type()])
  end

  def name_type() do
    case System.get_env("RABBITMQ_USE_LONGNAME") do
        "true" -> :longnames;
        _      -> :shortnames
    end
  end

  #
  # Implementation
  #

  defp start(_opt, 0, last_err) do
    {:error, last_err}
  end

  defp start(names_opt, attempts, _last_err) do
    candidate = String.to_atom("rabbitmqcli" <>
                               to_string(:rabbit_misc.random(100)))
    case :net_kernel.start([candidate | names_opt]) do
      {:ok, _} = ok    -> ok;
      {:error, reason} -> start(names_opt, attempts - 1, reason)
    end
  end
end
