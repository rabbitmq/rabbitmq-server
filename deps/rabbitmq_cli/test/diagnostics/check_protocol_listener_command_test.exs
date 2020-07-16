## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule CheckProtocolListenerCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.CheckProtocolListenerCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

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

  test "validate: when no arguments are provided, returns a failure" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: when two or more arguments are provided, returns a failure" do
    assert @command.validate(["amqp", "stomp"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: treats a single positional argument and default switches as a success" do
    assert @command.validate(["mqtt"], %{}) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?({:badrpc, _}, @command.run(["stomp"], Map.merge(context[:opts], %{node: :jake@thedog})))
  end

  test "run: when a listener for the protocol is active, returns a success", context do
    assert match?({true, _}, @command.run(["amqp"], context[:opts]))
  end

  test "run: accepts a number of alternative protocol names/spellings", context do
    for p <- ["amqp", "amqp1.0", "amqp10", "amqp091", "stomp1.2", "distribution"] do
      assert match?({true, _}, @command.run([p], context[:opts]))
    end
  end

  test "run: when a listener for the protocol is not active or unknown, returns an error", context do
    assert match?({false, _, _}, @command.run(["non-existent-proto"], context[:opts]))
  end

  test "output: when a listener for the protocol is active, returns a success", context do
    assert match?({:ok, _}, @command.output({true, "amqp"}, context[:opts]))
  end

  test "output: when a listener for the protocol is not active, returns an error", context do
    assert match?({:error, _}, @command.output({false, "http", []}, context[:opts]))
  end
end
