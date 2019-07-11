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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.AddUserCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes, Helpers, Input}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults

  def validate(args, _) when length(args) < 1, do: {:validation_failure, :not_enough_args}
  def validate(args, _) when length(args) > 2, do: {:validation_failure, :too_many_args}
  def validate([_], _), do: :ok
  def validate(["", _], _) do
    {:validation_failure, {:bad_argument, "user cannot be an empty string"}}
  end
  def validate([_, _], _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([username], %{node: node_name} = opts) do
    # note: blank passwords are currently allowed, they make sense
    # e.g. when a user only authenticates using X.509 certificates.
    # Credential validators can be used to require passwords of a certain length
    # or following a certain pattern. This is a core server responsibility. MK.
    case Input.infer_password("Password: ", opts) do
      :eof     -> {:error, :not_enough_args}
      password -> :rabbit_misc.rpc_call(
                    node_name,
                    :rabbit_auth_backend_internal,
                    :add_user,
                    [username, password, Helpers.cli_acting_user()]
                  )
    end
  end
  def run([username, password], %{node: node_name}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_auth_backend_internal,
      :add_user,
      [username, password, Helpers.cli_acting_user()]
    )
  end

  def usage, do: "add_user <username> <password>"

  def usage_additional() do
    [
      ["<username>", "Self-explanatory"],
      ["<password>", "Password this user will authenticate with. Use a blank string to disable password-based authentication."]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.access_control()
    ]
  end

  def help_section(), do: :user_management

  def description(), do: "Creates a new user in the internal database"

  def banner([username | _], _), do: "Adding user \"#{username}\" ..."

  def output({:error, :not_enough_args}, _) do
    {:error, ExitCodes.exit_software(), "Password is not provided via argument or stdin"}
  end
  def output({:error, {:user_already_exists, username}}, _) do
    {:error, ExitCodes.exit_software(), "User \"#{username}\" already exists"}
  end

  use RabbitMQ.CLI.DefaultOutput
end
