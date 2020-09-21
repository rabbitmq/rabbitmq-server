## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.PeekCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Queues.Commands.PeekCommand
  @invalid_position {:validation_failure, "position value must be a positive integer"}

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


  test "validate: treats no arguments as a failure" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: treats a single positional argument as a failure" do
    assert @command.validate(["quorum-queue-a"], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: when two or more arguments are provided, returns a failure" do
    assert @command.validate(["quorum-queue-a", "1"], %{}) == :ok
    assert @command.validate(["quorum-queue-a", "extra-arg", "another-extra-arg"], %{}) ==
             {:validation_failure, :too_many_args}
  end

  test "validate: when position is a negative number, returns a failure" do
    assert @command.validate(["quorum-queue-a", "-1"], %{}) == @invalid_position
  end

  test "validate: when position is zero, returns a failure" do
    assert @command.validate(["quorum-queue-a", "0"], %{}) == @invalid_position
  end

  test "validate: when position cannot be parsed to an integer, returns a failure" do
    assert @command.validate(["quorum-queue-a", "third"], %{}) == @invalid_position
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc" do
    assert match?({:badrpc, _}, @command.run(["quorum-queue-a", "1"],
                                             %{node: :jake@thedog, vhost: "/", timeout: 200}))
  end
end
