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

alias RabbitMQ.CLI.Core.Helpers, as: Helpers

defmodule RabbitMQ.CLI.Ctl.Commands.WaitCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  @default_timeout 10_000

  def merge_defaults(args, opts) do
    timeout = case opts[:timeout] do
      nil       -> @default_timeout;
      :infinity -> @default_timeout;
      val       -> val
    end
    {args, Map.put(opts, :timeout, timeout)}
 end

  def validate([_|_] = args, _) when length(args) > 1, do: {:validation_failure, :too_many_args}
  def validate([_], %{pid: _}), do: {:validation_failure, "Cannot specify both pid and pidfile"}
  def validate([], %{pid: _}), do: :ok
  def validate([], _), do: {:validation_failure, "No pid or pidfile specified"}
  def validate([_], _), do: :ok

  def switches(), do: [pid: :integer]

  def aliases(), do: ['P': :pid]

  def scopes(), do: [:ctl, :diagnostics]


  def run([pid_file], %{node: node_name, timeout: timeout}) do
    app_names = :rabbit_and_plugins

    Helpers.stream_until_error_parametrised(
      [
        log("Waiting for pid file '#{pid_file}' to appear"),
        fn(_)   -> wait_for_pid_file(pid_file, timeout) end,
        log_param(fn(pid) -> "pid is #{pid}" end),
      ]
      ++
      wait_for_pid_funs(node_name, app_names, timeout),
      :init)
  end
  def run([], %{node: node_name, pid: pid, timeout: timeout}) do
    app_names = :rabbit_and_plugins

    Helpers.stream_until_error_parametrised(
      wait_for_pid_funs(node_name, app_names, timeout),
      pid)
  end

  defp wait_for_pid_funs(node_name, app_names, timeout) do
    app_names_formatted = :io_lib.format('~p', [app_names])
    [
      log_param(fn(pid) -> "Waiting for erlang distribution on node '#{node_name}' while OS process '#{pid}' is running" end),
      fn(pid) -> wait_for_erlang_distribution(pid, node_name, timeout) end,
      log("Waiting for applications '#{app_names_formatted}' to start on node '#{node_name}'"),
      fn(_)   -> wait_for_application(node_name, app_names) end,
      log("Applications '#{app_names_formatted}' are running on node '#{node_name}'")
    ]
  end

  defp log(string) do
    fn(val) -> {:ok, val, string} end
  end

  defp log_param(fun) do
    fn(val) -> {:ok, val, fun.(val)} end
  end

  def usage, do: "wait [<pid_file>] [--pid|-P <pid>]"

  def banner(_, %{node: node_name}), do: "Waiting for node #{node_name} ..."

  def output({:error, err}, _opts) do
    case format_error(err) do
      :undefined -> RabbitMQ.CLI.DefaultOutput.output({:error, err});
      error_str  -> {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software, error_str}
    end
  end
  def output({:stream, stream}, _opts) do
    {:stream,
     Stream.map(stream, fn
                        ({:error, err}) ->
                          {:error,
                           case format_error(err) do
                             :undefined -> err;
                             error_str  -> error_str
                           end};
                        (other) -> other
                        end)}
  end
  use RabbitMQ.CLI.DefaultOutput

  defp format_error(:process_not_running) do
    "Error: process is not running."
  end
  defp format_error({:garbage_in_pid_file, _}) do
    "Error: garbage in pid file."
  end
  defp format_error({:could_not_read_pid, err}) do
    "Error: could not read pid. Detail: #{err}"
  end
  defp format_error(_) do
    :undefined
  end

  defp wait_for_application(node_name, :rabbit_and_plugins) do
    case :rpc.call(node_name, :rabbit, :await_startup, []) do
      :ok               -> :ok;
      {:badrpc, reason} -> {:error, {:badrpc, reason}}
    end
  end

  defp wait_for_erlang_distribution(pid, node_name, timeout) do
    wait_for(timeout,
      fn() ->
        case check_distribution(pid, node_name) do
          # Loop while node is available.
          {:error, :pang} -> {:error, :loop};
          other           -> other
        end
      end)
  end

  defp check_distribution(pid, node_name) do
    case is_os_process_alive(pid) do
      true ->
        case Node.ping(node_name) do
          :pong -> :ok;
          :pang -> {:error, :pang}
        end;
      false -> {:error, :process_not_running}
    end
  end

  defp is_os_process_alive(pid) do
    :rabbit_misc.is_os_process_alive(to_charlist(pid))
  end

  defp wait_for_pid_file(pid_file, timeout) do
    wait_for(timeout,
      fn() ->
        case :file.read_file(pid_file) do
          {:ok, bin} ->
            case Integer.parse(bin) do
              :error ->
                {:error, {:garbage_in_pid_file, pid_file}}
              {int, _} -> {:ok, int}
            end
          {:error, :enoent} ->
            {:error, :loop};
          {:error, err} ->
            {:error, {:could_not_read_pid, err}}
        end
      end)
  end

  def wait_for(timeout, fun)  do
    sleep = round(timeout / 10)
    case wait_for_loop(timeout, sleep, fun) do
      {:error, :timeout} -> {:error, {:timeout, timeout}}
      other              -> other
    end
  end

  def wait_for_loop(timeout, _, _) when timeout <= 0 do
    {:error, :timeout}
  end
  def wait_for_loop(timeout, sleep, fun) do
    case fun.() do
      {:error, :loop} ->
        :timer.sleep(sleep)
        wait_for_loop(timeout - sleep, sleep, fun);
      other -> other
    end
  end
end
