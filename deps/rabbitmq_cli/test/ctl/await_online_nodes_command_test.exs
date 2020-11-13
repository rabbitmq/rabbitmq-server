## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule AwaitOnlineNodesCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.AwaitOnlineNodesCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    {:ok, opts: %{node: get_rabbit_hostname(), timeout: 300_000}}
  end

  setup context do
    on_exit(context, fn -> delete_vhost(context[:vhost]) end)
    :ok
  end

  test "validate: wrong number of arguments results in arg count errors" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["1", "1"], %{}) == {:validation_failure, :too_many_args}
  end

  test "run: a call with node count of 1 with a running RabbitMQ node succeeds", context do
    assert @command.run(["1"], context[:opts]) == :ok
  end

  test "run: a call to an unreachable RabbitMQ node returns a nodedown" do
    opts   = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run(["1"], opts))
  end

  test "banner", context do
    assert @command.banner(["1"], context[:opts])
      =~ ~r/Will wait for at least 1 nodes to join the cluster of #{context[:opts][:node]}. Timeout: 300 seconds./
  end

end
