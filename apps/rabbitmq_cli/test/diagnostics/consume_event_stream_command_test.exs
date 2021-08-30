## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule ConsumeEventStreamCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.ConsumeEventStreamCommand

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
        duration: :infinity,
        pattern: ".*"
      }}
  end

  test "merge_defaults: duration defaults to infinity, pattern to anything" do
    assert @command.merge_defaults([], %{}) == {[], %{duration: :infinity,
                                                      pattern: ".*",
                                                      quiet: true}}
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

  test "run: consumes events for N seconds", context do

    stream = @command.run([], Map.merge(context[:opts], %{duration: 5}))
    :rpc.call(get_rabbit_hostname(), :rabbit_event, :notify, [String.to_atom("event_type1"),
                                                              [{String.to_atom("args"), 1}]])
    :rpc.call(get_rabbit_hostname(), :rabbit_event, :notify, [String.to_atom("event_type2"),
                                                              [{String.to_atom("pid"), self()}]])


    event1 =  Enum.find(stream, nil, fn x -> Keyword.get(x, :event, nil) == "event.type1" end)
    event2 =  Enum.find(stream, nil, fn x -> Keyword.get(x, :event, nil) == "event.type2" end)
    assert event1 != nil
    assert event2 != nil
    assert Keyword.get(event1, :args, nil) == 1
    assert is_binary(Keyword.get(event2, :pid, nil))

  end

end
