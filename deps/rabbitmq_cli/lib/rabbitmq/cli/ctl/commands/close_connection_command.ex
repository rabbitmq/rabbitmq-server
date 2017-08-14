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


defmodule RabbitMQ.CLI.Ctl.Commands.CloseConnectionCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  def merge_defaults(args, opts), do: {args, opts}

  def validate(args, _) when length(args) > 2, do: {:validation_failure, :too_many_args}
  def validate(args, _) when length(args) < 2, do: {:validation_failure, :not_enough_args}
  def validate([_,_], _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning
  
  def run([pid, explanation], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_networking,
      :close_connection,
      [:rabbit_misc.string_to_pid(pid), explanation])
  end

  def usage, do: "close_connection <connectionpid> <explanation>"

  def banner([pid, explanation], _), do: "Closing connection #{pid}, reason: #{explanation}..."
end
