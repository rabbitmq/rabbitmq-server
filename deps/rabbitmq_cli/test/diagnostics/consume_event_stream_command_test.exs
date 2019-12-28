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
## Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.

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
