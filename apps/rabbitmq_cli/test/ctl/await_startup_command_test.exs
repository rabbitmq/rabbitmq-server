## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
# Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule AwaitStartupCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.AwaitStartupCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    {:ok, opts: %{node: get_rabbit_hostname(), timeout: 300_000}}
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "merge_defaults: default timeout is 5 minutes" do
    assert @command.merge_defaults([], %{}) == {[], %{timeout: 300_000}}
  end

  test "validate: accepts no arguments", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate(["extra"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  test "run: request to a non-existent node returns a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "run: request to a fully booted node succeeds", context do
    # this timeout value is in seconds
    assert @command.run([], Map.merge(context[:opts], %{timeout: 5})) == :ok
  end

  test "empty banner", context do
    nil = @command.banner([], context[:opts])
  end
end
