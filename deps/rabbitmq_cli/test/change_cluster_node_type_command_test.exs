## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.


defmodule ChangeClusterNodeTypeCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ChangeClusterNodeTypeCommand

  setup_all do
    RabbitMQ.CLI.Distribution.start()
    :net_kernel.connect_node(get_rabbit_hostname)

    start_rabbitmq_app

    on_exit([], fn ->
      start_rabbitmq_app
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    :ok
  end

  setup do
    {:ok, opts: %{
      node: get_rabbit_hostname,
      disc: true,
      ram: false,
    }}
  end

  test "validate: argument can be either disc or ram", context do
    assert match?(
      {:validation_failure, {:bad_argument, _}},
      @command.validate(["foo"], context[:opts]))
    assert match?(:ok, @command.validate(["ram"], context[:opts]))
    assert match?(:ok, @command.validate(["disc"], context[:opts]))
  end
  test "validate: specifying no arguments is reported as an error", context do
    assert @command.validate([], context[:opts]) ==
      {:validation_failure, :not_enough_args}
  end
  test "validate: specifying multiple arguments is reported as an error", context do
    assert @command.validate(["a", "b", "c"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  # TODO
  #test "run: change ram node to disc node", context do
  #end

  # TODO
  #test "run: change disk node to ram node", context do
  #end

  test "run: request to an active node fails", context do
   assert match?(
     {:change_node_type_failed, {:mnesia_unexpectedly_running, _}},
    @command.run(["ram"], context[:opts]))
  end

  test "run: request to a non-existent node returns nodedown", context do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{
      node: target,
      disc: true,
      ram: false,
    }
    # We use "self" node as the target. It's enough to trigger the error.
    assert match?(
      {:badrpc, :nodedown},
      @command.run(["ram"], opts))
  end

  test "banner", context do
    assert @command.banner(["ram"], context[:opts]) =~
      ~r/Turning #{get_rabbit_hostname} into a ram node/
  end
end
