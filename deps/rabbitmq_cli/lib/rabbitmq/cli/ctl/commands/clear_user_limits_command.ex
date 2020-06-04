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

defmodule RabbitMQ.CLI.Ctl.Commands.ClearUserLimitsCommand do

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments

  def run([username, limittype], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_auth_backend_internal, :clear_user_limits, [
      username,
      limittype
    ])
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def usage, do: "clear_user_limits username limittype"

##ToDo  def usage_doc_guides() do
##ToDo    [
##ToDo      DocGuide.virtual_hosts()
##ToDo    ]
##ToDo  end

##ToDo  def help_section(), do: :virtual_hosts

  def description(), do: "Clears user connection/channel limits"

  def banner([username, limittype], %{}) do
    "Clearing \"#{limittype}\" limit for user \"#{username}\" ..."
  end
end
