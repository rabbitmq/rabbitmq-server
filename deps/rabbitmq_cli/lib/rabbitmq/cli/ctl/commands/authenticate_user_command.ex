## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.AuthenticateUserCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes, Input}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults

  def validate(args, _) when length(args) < 1, do: {:validation_failure, :not_enough_args}
  def validate(args, _) when length(args) > 2, do: {:validation_failure, :too_many_args}
  def validate([_], _), do: :ok
  def validate([_, _], _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([user], %{node: node_name} = opts) do
    # note: blank passwords are currently allowed, they make sense
    # e.g. when a user only authenticates using X.509 certificates.
    # Credential validators can be used to require passwords of a certain length
    # or following a certain pattern. This is a core server responsibility. MK.
    case Input.infer_password("Password: ", opts) do
      :eof     -> {:error, :not_enough_args}
      password -> :rabbit_misc.rpc_call(
                    node_name,
                    :rabbit_access_control,
                    :check_user_pass_login,
                    [user, password]
                  )
    end
  end
  def run([user, password], %{node: node_name}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_access_control,
      :check_user_pass_login,
      [user, password]
    )
  end

  def usage, do: "authenticate_user <username> <password>"

  def usage_additional() do
    [
      ["<username>", "Username to use"],
      ["<password>", "Password to use. Can be entered via stdin in interactive mode."]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.access_control()
    ]
  end

  def help_section(), do: :user_management

  def description(), do: "Attempts to authenticate a user. Exits with a non-zero code if authentication fails."

  def banner([username | _], _), do: "Authenticating user \"#{username}\" ..."

  def output({:error, :not_enough_args}, _) do
    {:error, ExitCodes.exit_software(), "Password is not provided via argument or stdin"}
  end
  def output({:refused, user, msg, args}, _) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_dataerr(),
     "Error: failed to authenticate user \"#{user}\"\n" <>
       to_string(:io_lib.format(msg, args))}
  end
  def output({:ok, _user}, _) do
    {:ok, "Success"}
  end

  use RabbitMQ.CLI.DefaultOutput
end
