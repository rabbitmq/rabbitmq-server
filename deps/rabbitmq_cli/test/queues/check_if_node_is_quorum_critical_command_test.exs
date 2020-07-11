## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.CheckIfNodeIsQuorumCriticalCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Queues.Commands.CheckIfNodeIsQuorumCriticalCommand

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

  test "validate: accepts no positional arguments" do
    assert @command.validate([], %{}) == :ok
  end

  test "validate: any positional arguments fail validation" do
    assert @command.validate(["quorum-queue-a"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["quorum-queue-a", "two"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["quorum-queue-a", "two", "three"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc" do
    assert match?({:badrpc, _}, @command.run([], %{node: :jake@thedog, vhost: "/", timeout: 200}))
  end
end
