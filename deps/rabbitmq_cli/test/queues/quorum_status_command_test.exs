## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.QuorumStatusCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Queues.Commands.QuorumStatusCommand

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

  test "validate: accepts a single positional argument" do
    assert @command.validate(["quorum-queue-a"], %{}) == :ok
  end

  test "validate: when two or more arguments are provided, returns a failure" do
    assert @command.validate(["quorum-queue-a", "one-extra-arg"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["quorum-queue-a", "extra-arg", "another-extra-arg"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc" do
    assert match?({:badrpc, _}, @command.run(["quorum-queue-a"],
                                             %{node: :jake@thedog, vhost: "/", timeout: 200}))
  end
end
