## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule UpdateClusterNodesCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.UpdateClusterNodesCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    start_rabbitmq_app()

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    :ok
  end

  setup do
    {:ok,
     opts: %{
       node: get_rabbit_hostname()
     }}
  end

  test "run: no op", context do
    assert match?(
             :ok,
             @command.run([context[:opts][:node]], context[:opts])
           )
  end

  test "banner", context do
    assert @command.banner(["a"], context[:opts]) =~
             ~r/DEPRECATED. This command is a no-op./
  end
end
