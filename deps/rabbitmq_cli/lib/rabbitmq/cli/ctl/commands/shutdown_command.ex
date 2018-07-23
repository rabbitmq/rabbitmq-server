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


defmodule RabbitMQ.CLI.Ctl.Commands.ShutdownCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput
  alias RabbitMQ.CLI.Core.OsPid

  def formatter(), do: RabbitMQ.CLI.Formatters.String

  def merge_defaults(args, opts), do: {args, opts}

  def validate([], _), do: :ok
  def validate([_|_], _), do: {:validation_failure, :too_many_args}

  def run([], %{node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :os, :getpid, []) do
      pid when is_list(pid) ->
        shutdown_node_and_wait_pid_to_stop(node_name, pid)
      other -> other
    end
  end

  def shutdown_node_and_wait_pid_to_stop(node_name, pid) do
    {:stream,
      RabbitMQ.CLI.Core.Helpers.stream_until_error([
        fn() -> "Shutting down RabbitMQ node #{node_name} running at PID #{pid}" end,
        fn() ->
          res = :rabbit_misc.rpc_call(node_name, :rabbit, :stop_and_halt, [])
          case res do
            :ok               -> "Waiting for PID #{pid} to terminate";
            {:badrpc, err}    -> {:error, err}
            {:error, _} = err -> err
          end
        end,
        fn() ->
          OsPid.wait_for_os_process_death(pid)
          "RabbitMQ node #{node_name} running at PID #{pid} successfully shut down"
        end])}
  end

  def usage, do: "shutdown"

  def banner(_, _), do: nil
end
