## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.GrowCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Queues.Commands.GrowCommand

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
       membership: "promotable",
       errors_only: false
     }}
  end

  test "merge_defaults: defaults to reporting complete results" do
    assert @command.merge_defaults([], %{}) ==
             {[],
              %{
                vhost_pattern: ".*",
                queue_pattern: ".*",
                errors_only: false,
                membership: "promotable"
              }}
  end

  test "validate: when no arguments are provided, returns a failure" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: when one argument is provided, returns a failure" do
    assert @command.validate(["quorum-queue-a"], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: when a node and even are provided, returns a success" do
    assert @command.validate(["quorum-queue-a", "even"], %{}) == :ok
  end

  test "validate: when a node and all are provided, returns a success" do
    assert @command.validate(["quorum-queue-a", "all"], %{}) == :ok
  end

  test "validate: when a node and something else is provided, returns a failure" do
    assert @command.validate(["quorum-queue-a", "banana"], %{}) ==
             {:validation_failure, "strategy 'banana' is not recognised."}
  end

  test "validate: when three arguments are provided, returns a failure" do
    assert @command.validate(["quorum-queue-a", "extra-arg", "another-extra-arg"], %{}) ==
             {:validation_failure, :too_many_args}
  end

  test "validate: when membership promotable is provided, returns a success" do
    assert @command.validate(["quorum-queue-a", "all"], %{membership: "promotable"}) == :ok
  end

  test "validate: when membership voter is provided, returns a success" do
    assert @command.validate(["quorum-queue-a", "all"], %{membership: "voter"}) == :ok
  end

  test "validate: when membership non_voter is provided, returns a success" do
    assert @command.validate(["quorum-queue-a", "all"], %{membership: "non_voter"}) == :ok
  end

  test "validate: when wrong membership is provided, returns failure" do
    assert @command.validate(["quorum-queue-a", "all"], %{membership: "banana"}) ==
             {:validation_failure, "voter status 'banana' is not recognised."}
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?(
             {:badrpc, _},
             @command.run(
               ["quorum-queue-a", "all"],
               Map.merge(context[:opts], %{node: :jake@thedog})
             )
           )
  end
end
