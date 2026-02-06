## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule DiagnosticsHasReachedTargetClusterSizeCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Upgrade.Commands.HasReachedTargetClusterSizeCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    start_rabbitmq_app()

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    :ok
  end

  setup context do
    {:ok,
     opts: %{
       node: get_rabbit_hostname(),
       timeout: context[:test_timeout] || 30000
     }}
  end

  test "scopes include diagnostics" do
    assert Enum.member?(@command.scopes(), :diagnostics)
  end

  test "scopes include upgrade" do
    assert Enum.member?(@command.scopes(), :upgrade)
  end

  test "scopes include ctl" do
    assert Enum.member?(@command.scopes(), :ctl)
  end

  test "run: returns true on a running single node", context do
    await_rabbitmq_startup()

    assert @command.run([], context[:opts]) == true
  end
end
