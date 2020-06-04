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
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SetUserLimitsCommand do

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults

  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([username, definition], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_auth_backend_internal, :set_user_limits, [
      username,
      definition
    ])
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "set_user_limits username <definition>"

  def usage_additional() do
    [
      ["<definition>", "Limit definitions, must be a valid JSON document"]
    ]
  end

  def description(), do: "Sets user limits"

  def banner([username, definition], %{}) do
    "Setting user limits to \"#{definition}\" for user \"#{username}\" ..."
  end
end
