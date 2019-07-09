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

defmodule RabbitMQ.CLI.Ctl.Commands.AuthenticateUserCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Input}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches, do: [interactive: :boolean]
  def aliases, do: [i: :interactive]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{interactive: true}, opts)}
  end

  def validate(args, _) when length(args) < 1, do: {:validation_failure, :not_enough_args}
  def validate(args, _) when length(args) > 2, do: {:validation_failure, :too_many_args}
  def validate([_], %{interactive: false}) do
    {:validation_failure, {:not_enough_args, "password must be provided as an argument in non-interactive mode"}}
  end
  def validate([_], _), do: :ok
  def validate([_, _], _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([user], %{node: node_name} = opts) do
    password = Input.infer_password("Password: ", opts)

    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_access_control,
      :check_user_pass_login,
      [user, password]
    )
  end
  def run([user, password], %{node: node_name}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_access_control,
      :check_user_pass_login,
      [user, password]
    )
  end

  def usage, do: "authenticate_user <username> <password> [--interactive]"

  def usage_additional() do
    [
      ["<username>", "Username to use"],
      ["<password>", "Password to use. Can be entered via stdin in interactive mode."],
      ["--interactive", "Indicates that password might be provided via stdin. This is the default."],
      ["--no-interactive", "Indicates that password must not be read from stdin."]
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
