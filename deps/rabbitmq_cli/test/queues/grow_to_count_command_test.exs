## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.GrowToCountCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Queues.Commands.GrowToCountCommand

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
    assert @command.validate([5], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: when node count and even are provided, returns a success" do
    assert @command.validate([7, "even"], %{}) == :ok
  end

  test "validate: when node count and all are provided, returns a success" do
    assert @command.validate([5, "all"], %{}) == :ok
  end

  test "validate: when node count and something else is provided, returns a failure" do
    assert @command.validate([7, "banana"], %{}) ==
             {:validation_failure, "strategy 'banana' is not recognised."}
  end

  test "validate: when three arguments are provided, returns a failure" do
    assert @command.validate([7, "extra-arg", "another-extra-arg"], %{}) ==
             {:validation_failure, :too_many_args}
  end

  test "validate: when membership promotable is provided, returns a success" do
    assert @command.validate([5, "all"], %{membership: "promotable"}) == :ok
  end

  test "validate: when membership voter is provided, returns a success" do
    assert @command.validate([7, "all"], %{membership: "voter"}) == :ok
  end

  test "validate: when membership non_voter is provided, returns a success" do
    assert @command.validate([5, "all"], %{membership: "non_voter"}) == :ok
  end

  test "validate: when wrong membership is provided, returns failure" do
    assert @command.validate([7, "all"], %{membership: "banana"}) ==
             {:validation_failure, "voter status 'banana' is not recognised."}
  end

  test "validate: when node count greater than zero, returns a success" do
    assert @command.validate([7, "all"], %{membership: "voter"}) == :ok
  end

  test "validate: when node count is zero, returns failure" do
    assert @command.validate([0, "all"], %{membership: "voter"}) ==
             {:validation_failure, "node count '0' must be greater than 0."}
  end

  test "validate: when node count is less than zero, returns failure" do
    assert @command.validate([-1, "all"], %{membership: "voter"}) ==
             {:validation_failure, "node count '-1' must be greater than 0."}
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc when growing to a node count", context do
    assert match?(
             {:badrpc, _},
             @command.run(
               [5, "all"],
               Map.merge(context[:opts], %{node: :jake@thedog})
             )
           )
  end
end
