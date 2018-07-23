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
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Ctl.Commands.ListExchangesCommand do
  alias RabbitMQ.CLI.Ctl.{InfoKeys, RpcStream}

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  @info_keys ~w(name type durable auto_delete internal arguments policy)a

  def info_keys(), do: @info_keys

  def scopes(), do: [:ctl, :diagnostics]
  def switches(), do: [timeout: :integer]
  def aliases(), do: [t: :timeout]

  def merge_defaults([], opts) do
    merge_defaults(~w(name type), opts)
  end

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  def validate(args, _) do
      case InfoKeys.validate_info_keys(args, @info_keys) do
        {:ok, _} -> :ok
        err -> err
      end
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([_|_] = args, %{node: node_name, timeout: timeout, vhost: vhost}) do
      info_keys = InfoKeys.prepare_info_keys(args)
      RpcStream.receive_list_items(node_name, :rabbit_exchange, :info_all,
        [vhost, info_keys],
        timeout,
        info_keys)
  end

  def usage() do
      "list_exchanges [-p <vhost>] [<exchangeinfoitem> ...]"
  end

  def usage_additional() do
      "<exchangeinfoitem> must be a member of the list [" <>
      Enum.join(@info_keys, ", ") <> "]."
  end

  def banner(_,%{vhost: vhost}), do: "Listing exchanges for vhost #{vhost} ..."
end
