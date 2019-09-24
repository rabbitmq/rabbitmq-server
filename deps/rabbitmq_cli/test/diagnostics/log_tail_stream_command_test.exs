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

defmodule LogTailStreamCommandTest do
  use ExUnit.Case
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.LogTailStreamCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    start_rabbitmq_app()

    ExUnit.configure([max_cases: 1])

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 30000,
        duration: :infinity
      }}
  end

  test "merge_defaults: duration defaults to infinity" do
    assert @command.merge_defaults([], %{}) == {[], %{duration: :infinity}}
  end

  test "validate: treats positional arguments as a failure" do
    assert @command.validate(["extra-arg"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: treats empty positional arguments and default switches as a success", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?({:badrpc, _}, @command.run([], Map.merge(context[:opts], %{node: :jake@thedog, timeout: 100})))
  end

  test "run: streams messages for N seconds", context do
    ensure_log_file()
    time_before = System.system_time(:second)

    stream = @command.run([], Map.merge(context[:opts], %{duration: 15}))
    :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, ["Message"])
    :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, ["Message1"])
    :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, ["Message2"])
    :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, ["Message3"])

    # This may take a long time and fail with an ExUnit timeout
    data = Enum.join(stream)

    time_after = System.system_time(:second)

    assert String.match?(data, ~r/Message/)
    assert String.match?(data, ~r/Message1/)
    assert String.match?(data, ~r/Message2/)
    assert String.match?(data, ~r/Message3/)

    time_spent = time_after - time_before
    assert time_spent > 15
    # This my take longer then duration but not too long
    assert time_spent < 45
  end

  test "run: may return an error if there is no log", context do
    delete_log_files()
    {:error, :enoent} = @command.run([], Map.merge(context[:opts], %{duration: 5}))
  end

  def ensure_log_file() do
    [log|_] = :rpc.call(get_rabbit_hostname(), :rabbit_lager, :log_locations, [])
    ensure_file(log, 100)
  end

  def ensure_file(log, 0) do
    flunk("timed out trying to ensure the log file #{log}")
  end
  def ensure_file(log, attempts) do
    case File.exists?(log) do
      true -> :ok
      false ->
        :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, ["Ping"])
        :timer.sleep(100)
        ensure_file(log, attempts - 1)
    end
  end

  def delete_log_files() do
    [_|_] = logs = :rpc.call(get_rabbit_hostname(), :rabbit_lager, :log_locations, [])
    Enum.map(logs, fn(log) ->
      File.rm(log)
    end)
  end
end
