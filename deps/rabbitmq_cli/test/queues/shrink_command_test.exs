## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.ShrinkCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Queues.Commands.ShrinkCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 30000,
        errors_only: false
      }}
  end

  test "merge_defaults: defaults to reporting complete results" do
    assert @command.merge_defaults([], %{}) == {[], %{errors_only: false}}
  end

  test "validate: when no arguments are provided, returns a failure" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: when one argument is provided, returns a success" do
    assert @command.validate(["quorum-queue-a"], %{}) == :ok
  end

  test "validate: when three or more arguments are provided, returns a failure" do
    assert @command.validate(["quorum-queue-a", "one-extra-arg"], %{}) ==
      {:validation_failure, :too_many_args}
    assert @command.validate(["quorum-queue-a", "extra-arg", "another-extra-arg"], %{}) ==
      {:validation_failure, :too_many_args}
  end

  test "validate: treats one positional arguments and default switches as a success" do
    assert @command.validate(["quorum-queue-a"], %{}) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?({:badrpc, _}, @command.run(["quorum-queue-a"],
                                Map.merge(context[:opts], %{node: :jake@thedog, vhost: "/", timeout: 200})))
  end
end
