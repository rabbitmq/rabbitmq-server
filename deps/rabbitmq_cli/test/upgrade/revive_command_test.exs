## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule ReviveCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Upgrade.Commands.ReviveCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    revive_node()

    on_exit(fn ->
      revive_node()
    end)

    :ok
  end

  setup context do
    enable_feature_flag(:maintenance_mode_status)

    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 5000
      }}
  end

  test "merge_defaults: nothing to do" do
    assert @command.merge_defaults([], %{}) == {[], %{}}
  end

  test "validate: accepts no positional arguments" do
    assert @command.validate(["extra-arg"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: succeeds with no positional arguments" do
    assert @command.validate([], %{}) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run([], Map.merge(context[:opts], opts)))
  end

  test "run: puts target node into regular operating mode", context do
    assert not is_draining_node()
    drain_node()
    await_condition(fn -> is_draining_node() end, 7000)
    assert :ok == @command.run([], context[:opts])
    await_condition(fn -> not is_draining_node() end, 7000)
  end
end
