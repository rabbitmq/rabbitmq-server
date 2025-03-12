## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule CheckForQuorumQueuesWithoutAnElectedLeaderCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.CheckForQuorumQueuesWithoutAnElectedLeaderCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok,
     opts: %{
       node: get_rabbit_hostname(),
       timeout: context[:test_timeout] || 30000
     }}
  end

  test "validate: treats no arguments as a failure" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: accepts a single positional argument" do
    assert @command.validate(["quorum.queue.*"], %{}) == :ok
  end

  test "validate: when two or more arguments are provided, returns a failure" do
    assert @command.validate(["quorum.queue.*", "one-extra-arg"], %{}) ==
             {:validation_failure, :too_many_args}

    assert @command.validate(["quorum.queue.*", "extra-arg", "another-extra-arg"], %{}) ==
             {:validation_failure, :too_many_args}
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc" do
    assert match?(
            {:error, {:badrpc, :nodedown}},
             @command.run(
               ["quorum.queue.*"],
               %{node: :jake@thedog, vhost: "/", across_all_vhosts: false, timeout: 200}
             )
           )
  end
end
