## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.DeleteUserCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([username], %{node: node_name}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_auth_backend_internal,
      :delete_user,
      [username, Helpers.cli_acting_user()]
    )
  end

  def output({:error, {:no_such_user, username}}, %{node: node_name, formatter: "json"}) do
    {:error, %{"result" => "error", "node" => node_name, "message" => "User #{username} does not exists"}}
  end
  def output({:error, {:no_such_user, username}}, _) do
    {:error, ExitCodes.exit_nouser(), "User \"#{username}\" does not exist"}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "delete_user <username>"

  def usage_additional() do
    [
      ["<username>", "Name of the user to delete."]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.access_control()
    ]
  end

  def help_section(), do: :user_management

  def description(), do: "Removes a user from the internal database. Has no effect on users provided by external backends such as LDAP"

  def banner([arg], _), do: "Deleting user \"#{arg}\" ..."
end
