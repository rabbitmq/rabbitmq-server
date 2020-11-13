## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ShutdownCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ShutdownCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname(), timeout: 15}}
  end

  test "validate: accepts no arguments", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate(["extra"], context[:opts]) == {:validation_failure, :too_many_args}
  end

  test "validate: in wait mode, checks if local and target node hostnames match" do
    assert match?({:validation_failure, {:unsupported_target, _}},
                  @command.validate([], %{wait: true, node: :'rabbit@some.remote.hostname'}))
  end

  test "validate: in wait mode, always assumes @localhost nodes are local" do
    assert @command.validate([], %{wait: true, node: :rabbit@localhost}) == :ok
  end

  test "validate: in no wait mode, passes unconditionally", context do
    assert @command.validate([], Map.merge(%{wait: false}, context[:opts])) == :ok
  end

  test "run: request to a non-existent node returns a badrpc" do
    opts = %{node: :jake@thedog, wait: false, timeout: 200}
    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "empty banner", context do
    nil = @command.banner([], context[:opts])
  end
end
