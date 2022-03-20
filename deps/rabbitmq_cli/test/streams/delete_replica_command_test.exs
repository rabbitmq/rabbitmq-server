## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
##

defmodule RabbitMQ.CLI.Streams.Commands.DeleteReplicaCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Streams.Commands.DeleteReplicaCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 30000,
        vhost: "/"
      }}
  end


  test "validate: when no arguments are provided, returns a failure" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: when one argument is provided, returns a failure" do
    assert @command.validate(["stream-queue-a"], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: when three or more arguments are provided, returns a failure" do
    assert @command.validate(["stream-queue-a", "rabbit@new-node", "one-extra-arg"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["stream-queue-a", "rabbit@new-node", "extra-arg", "another-extra-arg"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: treats two positional arguments and default switches as a success" do
    assert @command.validate(["stream-queue-a", "rabbit@new-node"], %{}) == :ok
  end

  test "run: trying to delete the last member of a stream should fail and return a meaningful message", context do
    declare_stream("test_stream_1", "/")
    assert @command.run(["test_stream_1", get_rabbit_hostname()], context[:opts]) ==
             {:error, "Cannot delete the last member of a stream"}
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc" do
    assert match?({:badrpc, _}, @command.run(["stream-queue-a", "rabbit@new-node"],
                                             %{node: :jake@thedog, vhost: "/", timeout: 200}))
  end
end
