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

defmodule LogTailCommandTest do
  use ExUnit.Case
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.LogTailCommand

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
        number: 50
      }}
  end

  test "merge_defaults: number is 50" do
    assert @command.merge_defaults([], %{}) == {[], %{number: 50}}
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

  test "run: shows last 50 lines from the log by default", context do
    # Let Lager's log message rate lapse or else some messages
    # we assert on might be dropped. MK.
    Process.sleep(1000)
    clear_log_files()
    log_messages =
      Enum.map(:lists.seq(1, 50),
               fn(n) ->
                 message = "Getting log tail #{n}"
                 :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, [message])
                 message
               end)
    wait_for_log_message("Getting log tail 50")
    lines = @command.run([], context[:opts])
    assert Enum.count(lines) == 50

    Enum.map(Enum.zip(log_messages, lines),
             fn({message, line}) ->
               assert String.match?(line, Regex.compile!(message))
             end)
  end

  test "run: returns N lines", context do
    # Let Lager's log message rate lapse or else some messages
    # we assert on might be dropped. MK.
    Process.sleep(1000)

    ## Log a bunch of lines
    Enum.map(:lists.seq(1, 50),
             fn(n) ->
               message = "More lines #{n}"
               :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, [message])
               message
             end)
    wait_for_log_message("More lines 50")
    assert Enum.count(@command.run([], Map.merge(context[:opts], %{number: 20}))) == 20
    assert Enum.count(@command.run([], Map.merge(context[:opts], %{number: 30}))) == 30
    assert Enum.count(@command.run([], Map.merge(context[:opts], %{number: 40}))) == 40
  end

  test "run: may return less than N lines if N is high", context do
    # Let Lager's log message rate lapse or else some messages
    # we assert on might be dropped. MK.
    Process.sleep(1000)
    clear_log_files()
    ## Log a bunch of lines
    Enum.map(:lists.seq(1, 100),
             fn(n) ->
               message = "More lines #{n}"
               :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, [message])
               message
             end)
    wait_for_log_message("More lines 50")
    assert Enum.count(@command.run([], Map.merge(context[:opts], %{number: 50}))) == 50
    assert Enum.count(@command.run([], Map.merge(context[:opts], %{number: 200}))) < 200
  end

  def clear_log_files() do
    [_|_] = logs = :rpc.call(get_rabbit_hostname(), :rabbit_lager, :log_locations, [])
    Enum.map(logs, fn(log) ->
      File.write(log, "")
    end)
  end
end
