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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.ListenersCommand do
  @moduledoc """
  Displays all listeners on a node.

  Returns a code of 0 unless there were connectivity and authentication
  errors. This command is not meant to be used in health checks.
  """

  alias RabbitMQ.CLI.Core.Helpers
  import RabbitMQ.CLI.Diagnostics.Helpers, only: [listeners_on: 2,
                                                  listener_lines: 1,
                                                  listener_maps: 1,
                                                  listener_rows: 1]

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [timeout: :integer]
  def aliases(), do: [t: :timeout]

  def merge_defaults(args, opts), do: {args, opts}

  def validate(args, _) when length(args) > 0 do
    {:validation_failure, :too_many_args}
  end
  def validate(_, _), do: :ok
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning


  def run([], %{node: node_name, timeout: timeout}) do
    # Example listener list:
    #
    # [{listener,rabbit@warp10,clustering,"localhost",
    #           {0,0,0,0,0,0,0,0},
    #           25672,[]},
    # {listener,rabbit@warp10,amqp,"localhost",
    #           {0,0,0,0,0,0,0,0},
    #           5672,
    #           [{backlog,128},
    #            {nodelay,true},
    #            {linger,{true,0}},
    #            {exit_on_close,false}]},
    # {listener,rabbit@warp10,stomp,"localhost",
    #           {0,0,0,0,0,0,0,0},
    #           61613,
    #           [{backlog,128},{nodelay,true}]}]
    case :rabbit_misc.rpc_call(node_name,
      :rabbit_networking, :active_listeners, [], timeout) do
      {:error, _}    = err -> err
      {:error, _, _} = err -> err
      xs when is_list(xs)  -> listeners_on(xs, node_name)
      other                -> other
    end
  end

  def output([], %{node: node_name, formatter: "json"}) do
    {:ok, %{"result"    => "ok",
            "node"      => node_name,
            "listeners" => []}}
  end
  def output([], %{node: node_name, formatter: "csv"}) do
    {:ok, []}
  end  
  def output([], %{node: node_name}) do
    {:ok, "Node #{node_name} reported no enabled listeners."}
  end
  def output(listeners, %{formatter: "json"}) do
    {:ok, %{"result"    => "ok",
            "listeners" => listener_maps(listeners)}}
  end
  def output(listeners, %{formatter: "csv"}) do
    {:stream, [listener_rows(listeners)]}
  end  
  def output(listeners, _opts) do
    lines = listener_lines(listeners)

    {:ok, Enum.join(lines, Helpers.line_separator())}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "listeners"

  def banner([], %{node: node_name}) do
    "Asking node #{node_name} to report its protocol listeners ..."
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.String
end
