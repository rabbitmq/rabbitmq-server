## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
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

defmodule RabbitMQ.CLI.Queues.Commands.ShrinkCommandTest do
  use ExUnit.Case
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
    assert @command.run(["quorum-queue-a"],
                        Map.merge(context[:opts], %{node: :jake@thedog, vhost: "/"})) ==
                          {:badrpc, :nodedown}
  end
end
