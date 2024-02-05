## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule RenameClusterNodeCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.RenameClusterNodeCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    node = get_rabbit_hostname()

    start_rabbitmq_app()

    {:ok, plugins_dir} =
      :rabbit_misc.rpc_call(node, :application, :get_env, [:rabbit, :plugins_dir])

    rabbitmq_home = :rabbit_misc.rpc_call(node, :code, :lib_dir, [:rabbit])
    data_dir = :rabbit_misc.rpc_call(node, :rabbit, :data_dir, [])

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    {:ok, opts: %{rabbitmq_home: rabbitmq_home, plugins_dir: plugins_dir, data_dir: data_dir}}
  end

  setup context do
    {:ok,
     opts:
       Map.merge(
         context[:opts],
         %{node: :not_running@localhost}
       )}
  end

  test "banner", context do
    assert @command.banner(["a", "b"], context[:opts]) =~
             ~r/DEPRECATED. This command is a no-op./
  end
end
