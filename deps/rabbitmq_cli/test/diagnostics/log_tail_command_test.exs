## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule LogTailCommandTest do
  use ExUnit.Case, async: false
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
    clear_log_files()
    log_messages =
      Enum.map(:lists.seq(1, 50),
               fn(n) ->
                 message = "Getting log tail #{n}"
                 :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, [to_charlist(message)])
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
    ## Log a bunch of lines
    Enum.map(:lists.seq(1, 50),
             fn(n) ->
               message = "More lines #{n}"
               :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, [to_charlist(message)])
               message
             end)
    wait_for_log_message("More lines 50")
    assert Enum.count(@command.run([], Map.merge(context[:opts], %{number: 20}))) == 20
    assert Enum.count(@command.run([], Map.merge(context[:opts], %{number: 30}))) == 30
    assert Enum.count(@command.run([], Map.merge(context[:opts], %{number: 40}))) == 40
  end

  test "run: may return less than N lines if N is high", context do
    clear_log_files()
    ## Log a bunch of lines
    Enum.map(:lists.seq(1, 100),
             fn(n) ->
               message = "More lines #{n}"
               :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, [to_charlist(message)])
               message
             end)
    wait_for_log_message("More lines 50")
    assert Enum.count(@command.run([], Map.merge(context[:opts], %{number: 50}))) == 50
    assert Enum.count(@command.run([], Map.merge(context[:opts], %{number: 200}))) < 200
  end

  def clear_log_files() do
    [_|_] = logs = :rpc.call(get_rabbit_hostname(), :rabbit, :log_locations, [])
    Enum.map(logs, fn(log) ->
      case log do
        '<stdout>' -> :ok
        _          -> File.write(log, "")
      end
    end)
  end
end
