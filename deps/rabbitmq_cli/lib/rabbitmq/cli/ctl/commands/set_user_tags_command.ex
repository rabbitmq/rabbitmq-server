## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SetUserTagsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts), do: {args, opts}

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end
  def validate(_, _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([user | tags], %{node: node_name}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_auth_backend_internal,
      :set_tags,
      [user, tags, Helpers.cli_acting_user()]
    )
  end

  def output({:error, {:no_such_user, username}}, %{node: node_name, formatter: "json"}) do
    {:error, %{"result" => "error", "node" => node_name, "message" => "User #{username} does not exists"}}
  end
  def output({:error, {:no_such_user, username}}, _) do
    {:error, ExitCodes.exit_nouser(), "User \"#{username}\" does not exist"}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "set_user_tags <username> <tag> [...]"

  def usage_additional() do
    [
      ["<username>", "Self-explanatory"],
      ["<tags>", "Space separated list of tags"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.management(),
      DocGuide.access_control()
    ]
  end

  def help_section(), do: :user_management

  def description(), do: "Sets user tags"

  def banner([user | tags], _) do
    "Setting tags for user \"#{user}\" to [#{tags |> Enum.join(", ")}] ..."
  end
end
