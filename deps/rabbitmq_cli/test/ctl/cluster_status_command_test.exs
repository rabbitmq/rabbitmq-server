## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ClusterStatusCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ClusterStatusCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname(), timeout: 12000}}
  end

  test "validate: argument count validates", context do
    assert @command.validate([], context[:opts]) == :ok
    assert @command.validate(["extra"], context[:opts]) == {:validation_failure, :too_many_args}
  end

  test "run: status request to a reachable node returns cluster information", context do
    n = context[:opts][:node]
    res = @command.run([], context[:opts])

    assert Enum.member?(res[:nodes][:disc], n)
    assert res[:partitions] == []
    assert res[:alarms][n] == []
  end

  test "run: status request on nonexistent RabbitMQ node returns a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "banner", context do
    s = @command.banner([], context[:opts])

    assert s =~ ~r/Cluster status of node/
    assert s =~ ~r/#{get_rabbit_hostname()}/
  end
end
