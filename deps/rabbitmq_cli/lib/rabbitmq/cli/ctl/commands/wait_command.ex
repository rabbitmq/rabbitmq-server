## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.WaitCommand do
  alias RabbitMQ.CLI.Core.{Helpers, Validators}

  @behaviour RabbitMQ.CLI.CommandBehaviour
  @default_timeout 10_000

  def scopes(), do: [:ctl, :diagnostics]

  def switches(), do: [pid: :integer, timeout: :integer]
  def aliases(), do: [P: :pid, t: :timeout]

  def merge_defaults(args, opts) do
    timeout =
      case opts[:timeout] do
        nil -> @default_timeout
        :infinity -> @default_timeout
        val -> val
      end

    {args, Map.put(opts, :timeout, timeout)}
  end

  def validate([_ | _] = args, _) when length(args) > 1, do: {:validation_failure, :too_many_args}
  def validate([_], %{pid: _}), do: {:validation_failure, "Cannot specify both pid and pidfile"}
  def validate([_], _), do: :ok
  def validate([], %{pid: _}), do: :ok
  def validate([], _), do: {:validation_failure, "No pid or pidfile specified"}

  def validate_execution_environment([], %{pid: _} = opts) do
    Validators.rabbit_is_loaded([], opts)
  end
  def validate_execution_environment([_pid_file], opts) do
    Validators.rabbit_is_loaded([], opts)
  end

  def run([pid_file], %{node: node_name, timeout: timeout} = opts) do
    app_names = :rabbit_and_plugins
    quiet = opts[:quiet] || false

    Helpers.stream_until_error_parameterised(
      [
        log("Waiting for pid file '#{pid_file}' to appear", quiet),
        fn _ -> wait_for_pid_file(pid_file, node_name, timeout) end,
        log_param(fn pid -> "pid is #{pid}" end, quiet)
      ] ++
        wait_for_pid_funs(node_name, app_names, timeout, quiet),
      :init
    )
  end

  def run([], %{node: node_name, pid: pid, timeout: timeout} = opts) do
    app_names = :rabbit_and_plugins
    quiet = opts[:quiet] || false

    Helpers.stream_until_error_parameterised(
      wait_for_pid_funs(node_name, app_names, timeout, quiet),
      pid
    )
  end

  def output({:error, err}, opts) do
    case format_error(err) do
      :undefined -> RabbitMQ.CLI.DefaultOutput.output({:error, err}, opts)
      error_str -> {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(), error_str}
    end
  end

  def output({:stream, stream}, _opts) do
    {:stream,
     Stream.map(stream, fn
       {:error, err} ->
         {:error,
          case format_error(err) do
            :undefined -> err
            error_str -> error_str
          end}

       other ->
         other
     end)}
  end

  use RabbitMQ.CLI.DefaultOutput

  # Banner is printed in wait steps
  def banner(_, _), do: nil

  def usage, do: "wait [<pidfile>] [--pid|-P <pid>]"

  def usage_additional() do
    [
      ["<pidfile>", "PID file path"],
      ["--pid <pid>", "operating system PID to monitor"]
    ]
  end

  def help_section(), do: :node_management

  def description(), do: "Waits for RabbitMQ node startup by monitoring a local PID file. See also 'rabbitmqctl await_online_nodes'"

  #
  # Implementation
  #

  def wait_for(timeout, fun) do
    sleep = 1000

    case wait_for_loop(timeout, sleep, fun) do
      {:error, :timeout} -> {:error, {:timeout, timeout}}
      other -> other
    end
  end

  def wait_for_loop(timeout, _, _) when timeout <= 0 do
    {:error, :timeout}
  end

  def wait_for_loop(timeout, sleep, fun) do
    time = :erlang.system_time(:milli_seconds)

    case fun.() do
      {:error, :loop} ->
        time_to_fun = :erlang.system_time(:milli_seconds) - time

        time_taken =
          case {time_to_fun > timeout, time_to_fun > sleep} do
            ## The function took longer than timeout
            {true, _} ->
              time_to_fun

            ## The function took longer than sleep
            {false, true} ->
              time_to_fun

            ## We need to sleep
            {false, false} ->
              :timer.sleep(sleep)
              time_to_fun + sleep
          end

        wait_for_loop(timeout - time_taken, sleep, fun)

      other ->
        other
    end
  end

  defp wait_for_pid_funs(node_name, app_names, timeout, quiet) do
    app_names_formatted = :io_lib.format('~p', [app_names])

    [
      log_param(
        fn pid ->
          "Waiting for erlang distribution on node '#{node_name}' while OS process '#{pid}' is running"
        end,
        quiet
      ),
      fn pid -> wait_for_erlang_distribution(pid, node_name, timeout) end,
      log(
        "Waiting for applications '#{app_names_formatted}' to start on node '#{node_name}'",
        quiet
      ),
      fn _ -> wait_for_application(node_name, app_names) end,
      log("Applications '#{app_names_formatted}' are running on node '#{node_name}'", quiet)
    ]
  end

  defp log(_string, _quiet = true) do
    fn val -> {:ok, val} end
  end

  defp log(string, _quiet = false) do
    fn val -> {:ok, val, string} end
  end

  defp log_param(_fun, _quiet = true) do
    fn val -> {:ok, val} end
  end

  defp log_param(fun, _quiet = false) do
    fn val -> {:ok, val, fun.(val)} end
  end

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
    case :rabbit.await_startup(node_name) do
      {:badrpc, err} -> {:error, {:badrpc, err}}
      other -> other
    end
  end

  defp wait_for_erlang_distribution(pid, node_name, timeout) do
    wait_for(
      timeout,
      fn ->
        case check_distribution(pid, node_name) do
          # Loop while node is available.
          {:error, :pang} -> {:error, :loop}
          other -> other
        end
      end
    )
  end

  defp check_distribution(pid, node_name) do
    case is_os_process_alive(pid) do
      true ->
        case Node.ping(node_name) do
          :pong -> :ok
          :pang -> {:error, :pang}
        end

      false ->
        {:error, :process_not_running}
    end
  end

  defp is_os_process_alive(pid) do
    :rabbit_misc.is_os_process_alive(to_charlist(pid))
  end

  defp wait_for_pid_file(pid_file, node_name, timeout) do
    wait_for(
      timeout,
      fn ->
        case :file.read_file(pid_file) do
          {:ok, bin} ->
            case Integer.parse(bin) do
              :error ->
                {:error, {:garbage_in_pid_file, pid_file}}

              {pid, _} ->
                case check_distribution(pid, node_name) do
                  :ok -> {:ok, pid}
                  _   -> {:error, :loop}
                end
            end

          {:error, :enoent} ->
            {:error, :loop}

          {:error, err} ->
            {:error, {:could_not_read_pid, err}}
        end
      end
    )
  end
end
