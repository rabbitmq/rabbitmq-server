## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.ForceCheckpointCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Queues.Commands.ForceCheckpointCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok,
     opts: %{
       node: get_rabbit_hostname(),
       timeout: context[:test_timeout] || 30000,
       vhost_pattern: ".*",
       queue_pattern: ".*",
       errors_only: false
     }}
  end

  test "merge_defaults: defaults to reporting complete results" do
    assert @command.merge_defaults([], %{}) ==
             {[],
              %{
                vhost_pattern: ".*",
                queue_pattern: ".*",
                errors_only: false
              }}
  end

  test "validate: accepts no positional arguments" do
    assert @command.validate([], %{}) == :ok
  end

  test "validate: any positional arguments fail validation" do
    assert @command.validate(["quorum-queue-a"], %{}) == {:validation_failure, :too_many_args}

    assert @command.validate(["quorum-queue-a", "two"], %{}) ==
             {:validation_failure, :too_many_args}

    assert @command.validate(["quorum-queue-a", "two", "three"], %{}) ==
             {:validation_failure, :too_many_args}
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?(
             {:badrpc, _},
             @command.run(
               [],
               Map.merge(context[:opts], %{node: :jake@thedog})
             )
           )
  end
end
