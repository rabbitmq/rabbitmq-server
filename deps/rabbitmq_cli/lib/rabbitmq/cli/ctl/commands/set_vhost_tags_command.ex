## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SetVhostTagsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts), do: {args, opts}

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end
  def validate(_, _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([vhost | tags], %{node: node_name}) do
    case :rabbit_misc.rpc_call(
      node_name, :rabbit_vhost, :update_tags, [vhost, tags, Helpers.cli_acting_user()]) do
      {:error, _}  = err -> err
      {:badrpc, _} = err -> err
      _                  -> :ok
    end
  end

  def output({:error, {:no_such_vhost, vhost}}, %{node: node_name, formatter: "json"}) do
    {:error, %{"result" => "error", "node" => node_name, "message" => "Virtual host \"#{vhost}\" does not exists"}}
  end
  def output({:error, {:no_such_vhost, vhost}}, _) do
    {:error, ExitCodes.exit_dataerr(), "Virtual host \"#{vhost}\" does not exist"}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "set_vhost_tags <vhost> <tag> [...]"

  def usage_additional() do
    [
      ["<vhost>", "Self-explanatory"],
      ["<tags>", "Space separated list of tags"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.virtual_hosts()
    ]
  end

  def help_section(), do: :virtual_hosts

  def description(), do: "Sets virtual host tags"

  def banner([vhost | tags], _) do
    "Setting tags for virtual host \"#{vhost}\" to [#{tags |> Enum.join(", ")}] ..."
  end
end
