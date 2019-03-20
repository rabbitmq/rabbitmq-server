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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.GrowCommandTest do
  use ExUnit.Case
  import TestHelper

  @command RabbitMQ.CLI.Queues.Commands.GrowCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok, opts: %{
      node: get_rabbit_hostname(),
      timeout: context[:test_timeout] || 30000,
      vhost_pattern: ".*",
      queue_pattern: ".*",
      errors_only: false
    }}
  end

  test "merge_defaults: defaults to reporting complete results" do
    assert @command.merge_defaults([], %{}) ==
      {[], %{vhost_pattern: ".*",
             queue_pattern: ".*",
             errors_only: false}}
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

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert @command.run(["quorum-queue-a", "all"],
                        Map.merge(context[:opts], %{node: :jake@thedog})) ==
                          {:badrpc, :nodedown}
  end
end
