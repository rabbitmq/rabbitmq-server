## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ChangePasswordCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes, Helpers, Input}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults

  def validate(args, _) when length(args) < 1, do: {:validation_failure, :not_enough_args}
  def validate(args, _) when length(args) > 2, do: {:validation_failure, :too_many_args}
  def validate([_], _), do: :ok
  def validate([_, _], _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([username], %{node: node_name} = opts) do
    # note: blank passwords are currently allowed, they make sense
    # e.g. when a user only authenticates using X.509 certificates.
    # Credential validators can be used to require passwords of a certain length
    # or following a certain pattern. This is a core server responsibility. MK.
    case Input.infer_password("Password: ", opts) do
      :eof -> {:error, :not_enough_args}
      password -> :rabbit_misc.rpc_call(
                    node_name,
                    :rabbit_auth_backend_internal,
                    :change_password,
                    [username, password, Helpers.cli_acting_user()]
                  )
    end
  end
  def run([username, password], %{node: node_name}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_auth_backend_internal,
      :change_password,
      [username, password, Helpers.cli_acting_user()]
    )
  end

  def output({:error, :not_enough_args}, _) do
    {:error, ExitCodes.exit_software(), "Password is not provided via argument or stdin"}
  end
  def output({:error, {:no_such_user, username}}, %{node: node_name, formatter: "json"}) do
    {:error, %{"result" => "error", "node" => node_name, "message" => "User #{username} does not exists"}}
  end
  def output({:error, {:no_such_user, username}}, _) do
    {:error, ExitCodes.exit_nouser(), "User \"#{username}\" does not exist"}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "change_password <username> <password>"

  def usage_additional() do
    [
      ["<username>", "Name of the user whose password should be changed"],
      ["<password>", "New password to set"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.access_control()
    ]
  end

  def help_section(), do: :user_management

  def description(), do: "Changes the user password"

  def banner([user | _], _), do: "Changing password for user \"#{user}\" ..."
end
