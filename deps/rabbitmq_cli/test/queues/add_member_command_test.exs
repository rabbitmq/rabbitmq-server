## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.AddMemberCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Queues.Commands.AddMemberCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok,
     opts: %{
       node: get_rabbit_hostname(),
       membership: "voter",
       vhost: "/",
       timeout: context[:test_timeout] || 30000
     }}
  end

  test "validate: when no arguments are provided, returns a failure" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: when one argument is provided, returns a failure" do
    assert @command.validate(["quorum-queue-a"], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: when three or more arguments are provided, returns a failure" do
    assert @command.validate(["quorum-queue-a", "rabbit@new-node", "one-extra-arg"], %{}) ==
             {:validation_failure, :too_many_args}

    assert @command.validate(
             ["quorum-queue-a", "rabbit@new-node", "extra-arg", "another-extra-arg"],
             %{}
           ) == {:validation_failure, :too_many_args}
  end

  test "validate: when membership promotable is provided, returns a success" do
    assert @command.validate(["quorum-queue-a", "rabbit@new-node"], %{membership: "promotable"}) ==
             :ok
  end

  test "validate: when membership voter is provided, returns a success" do
    assert @command.validate(["quorum-queue-a", "rabbit@new-node"], %{membership: "voter"}) == :ok
  end

  test "validate: when membership non_voter is provided, returns a success" do
    assert @command.validate(["quorum-queue-a", "rabbit@new-node"], %{membership: "non_voter"}) ==
             :ok
  end

  test "validate: when wrong membership is provided, returns failure" do
    assert @command.validate(["quorum-queue-a", "rabbit@new-node"], %{membership: "banana"}) ==
             {:validation_failure, "voter status 'banana' is not recognised."}
  end

  test "validate: treats two positional arguments and default switches as a success" do
    assert @command.validate(["quorum-queue-a", "rabbit@new-node"], %{}) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?(
             {:badrpc, _},
             @command.run(
               ["quorum-queue-a", "rabbit@new-node"],
               Map.merge(context[:opts], %{node: :jake@thedog})
             )
           )
  end
end
