## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule WaitCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.WaitCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    start_rabbitmq_app()

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    RabbitMQ.CLI.Core.Distribution.start()
    rabbitmq_home = :rabbit_misc.rpc_call(get_rabbit_hostname(), :code, :lib_dir, [:rabbit])

    {:ok, opts: %{node: get_rabbit_hostname(),
                  rabbitmq_home: rabbitmq_home,
                  timeout: 500}}
  end


  test "validate: cannot have both pid and pidfile", context do
    {:validation_failure, "Cannot specify both pid and pidfile"} =
      @command.validate(["pid_file"], Map.merge(context[:opts], %{pid: 123}))
  end

  test "validate: should have either pid or pidfile", context do
    {:validation_failure, "No pid or pidfile specified"} =
      @command.validate([], context[:opts])
  end

  test "validate: with more than one argument returns an arg count error", context do
    assert @command.validate(["pid_file", "extra"], context[:opts]) == {:validation_failure, :too_many_args}
  end

  test "run: times out waiting for non-existent pid file", context do
    {:error, {:timeout, _}} = @command.run(["pid_file"], context[:opts]) |> Enum.to_list |> List.last
  end

  test "run: fails if pid process does not exist", context do
    non_existent_pid = get_non_existent_os_pid()
    {:error, :process_not_running} =
      @command.run([], Map.merge(context[:opts], %{pid: non_existent_pid}))
      |> Enum.to_list
      |> List.last
  end

  test "run: times out if unable to communicate with the node", context do
    pid = String.to_integer(System.get_pid())
    {:error, {:timeout, _}} =
      @command.run([], Map.merge(context[:opts], %{pid: pid, node: :nonode@nohost}))
      |> Enum.to_list
      |> List.last
  end

  test "run: happy path", context do
    pid = :erlang.list_to_integer(:rpc.call(context[:opts][:node], :os, :getpid, []))
    output = @command.run([], Map.merge(context[:opts], %{pid: pid}))
    assert_stream_without_errors(output)
  end

  test "run: happy path in quiet mode", context do
    pid = :erlang.list_to_integer(:rpc.call(context[:opts][:node], :os, :getpid, []))
    output = @command.run([], Map.merge(context[:opts], %{pid: pid, quiet: true}))
    [] = Enum.to_list(output)
  end

  test "no banner", context do
    nil = @command.banner([], context[:opts])
  end

  test "output: process not running error", context do
    exit_code = RabbitMQ.CLI.Core.ExitCodes.exit_software
    assert match?({:error, ^exit_code, "Error: process is not running."},
                  @command.output({:error, :process_not_running}, context[:opts]))
  end

  test "output: garbage in pid file error", context do
    exit_code = RabbitMQ.CLI.Core.ExitCodes.exit_software
    assert match?({:error, ^exit_code, "Error: garbage in pid file."},
                  @command.output({:error, {:garbage_in_pid_file, "somefile"}}, context[:opts]))
  end

  test "output: could not read pid error", context do
    exit_code = RabbitMQ.CLI.Core.ExitCodes.exit_software
    assert match?({:error, ^exit_code, "Error: could not read pid. Detail: something wrong"},
                  @command.output({:error, {:could_not_read_pid, "something wrong"}}, context[:opts]))
  end

  test "output: default output is fine", context do
    assert match?({:error, "message"}, @command.output({:error, "message"}, context[:opts]))
    assert match?({:error, :message}, @command.output({:error, :message}, context[:opts]))
    assert match?({:error, :message}, @command.output(:message, context[:opts]))
    assert match?({:ok, "ok"}, @command.output({:ok, "ok"}, context[:opts]))
    assert match?(:ok, @command.output(:ok, context[:opts]))
    assert match?({:ok, "ok"}, @command.output("ok", context[:opts]))
  end

  def get_non_existent_os_pid(pid \\ 2) do
    case :rabbit_misc.is_os_process_alive(to_charlist(pid)) do
      true  -> get_non_existent_os_pid(pid + 1)
      false -> pid
    end
  end
end
