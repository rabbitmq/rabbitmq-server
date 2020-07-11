## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule ListenersCommandTest do
  use ExUnit.Case, async: false
  import TestHelper
  import RabbitMQ.CLI.Core.Listeners, only: [listener_maps: 1]

  @command RabbitMQ.CLI.Diagnostics.Commands.ListenersCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    start_rabbitmq_app()

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 30000
      }}
  end

  test "merge_defaults: nothing to do" do
    assert @command.merge_defaults([], %{}) == {[], %{}}
  end

  test "validate: treats positional arguments as a failure" do
    assert @command.validate(["extra-arg"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: treats empty positional arguments and default switches as a success" do
    assert @command.validate([], %{}) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?({:badrpc, _}, @command.run([], Map.merge(context[:opts], %{node: :jake@thedog})))
  end

  test "run: returns a list of node-local listeners", context do
    xs = @command.run([], context[:opts]) |> listener_maps

    assert length(xs) >= 3
    for p <- [5672, 61613, 25672] do
      assert Enum.any?(xs, fn %{port: port} -> port == p end)
    end
  end

  test "output: returns a formatted list of node-local listeners", context do
    raw        = @command.run([],  context[:opts])
    {:ok, msg} = @command.output(raw, context[:opts])

    for p <- [5672, 61613, 25672] do
      assert msg =~ ~r/#{p}/
    end
  end

  test "output: when formatter is JSON, returns an array of listener maps", context do
    raw        = @command.run([],  context[:opts])
    {:ok, doc} = @command.output(raw, Map.merge(%{formatter: "json"}, context[:opts]))
    xs         = doc["listeners"]

    assert length(xs) >= 3
    for p <- [5672, 61613, 25672] do
      assert Enum.any?(xs, fn %{port: port} -> port == p end)
    end
  end
end
