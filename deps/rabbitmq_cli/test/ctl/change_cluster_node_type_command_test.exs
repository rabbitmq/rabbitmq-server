## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ChangeClusterNodeTypeCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ChangeClusterNodeTypeCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    start_rabbitmq_app()

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    :ok
  end

  setup do
    {:ok, opts: %{
      node: get_rabbit_hostname()
    }}
  end

  test "validate: node type of disc, disk, and ram pass validation", context do
    assert match?(
      {:validation_failure, {:bad_argument, _}},
      @command.validate(["foo"], context[:opts]))

    assert :ok == @command.validate(["ram"], context[:opts])
    assert :ok == @command.validate(["disc"], context[:opts])
    assert :ok == @command.validate(["disk"], context[:opts])
  end

  test "validate: providing no arguments fails validation", context do
    assert @command.validate([], context[:opts]) ==
      {:validation_failure, :not_enough_args}
  end
  test "validate: providing too many arguments fails validation", context do
    assert @command.validate(["a", "b", "c"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  # TODO
  #test "run: change ram node to disc node", context do
  #end

  # TODO
  #test "run: change disk node to ram node", context do
  #end

  test "run: request to a node with running RabbitMQ app fails", context do
   assert match?(
     {:error, :mnesia_unexpectedly_running},
    @command.run(["ram"], context[:opts]))
  end

  test "run: request to an unreachable node returns a badrpc", _context do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?(
      {:badrpc, :nodedown},
      @command.run(["ram"], opts))
  end

  test "banner", context do
    assert @command.banner(["ram"], context[:opts]) =~
      ~r/Turning #{get_rabbit_hostname()} into a ram node/
  end

  test "output mnesia is running error", context do
    exit_code = RabbitMQ.CLI.Core.ExitCodes.exit_software
    assert match?({:error, ^exit_code,
                   "Mnesia is still running on node " <> _},
                   @command.output({:error, :mnesia_unexpectedly_running}, context[:opts]))

  end
end
