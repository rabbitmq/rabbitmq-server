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


defmodule RabbitMQ.CLI.Ctl.Commands.AddUserCommand do

  alias RabbitMQ.CLI.Core.Helpers, as: Helpers
  alias RabbitMQ.CLI.Core.ExitCodes, as: ExitCodes

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts), do: {args, opts}

  def validate(args, _) when length(args) < 2 do
    {:validation_failure, :not_enough_args}
  end

  def validate(args, _) when length(args) > 2 do
    {:validation_failure, :too_many_args}
  end

  def validate(["", _], _) do
    {:validation_failure, {:bad_argument, "user cannot be empty string."}}
  end

  def validate([_,_], _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([_, _] = args, %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name,
      :rabbit_auth_backend_internal,
      :add_user, args ++ [Helpers.cli_acting_user()])
  end

  def usage, do: "add_user <username> <password>"

  def banner([username, _password], _), do: "Adding user \"#{username}\" ..."

  def output({:error, {:user_already_exists, username}}, _) do
    {:error, ExitCodes.exit_software(), "User \"#{username}\" already exists"}
  end
  use RabbitMQ.CLI.DefaultOutput
end
