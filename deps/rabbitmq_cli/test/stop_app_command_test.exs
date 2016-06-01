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
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule StopAppCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command StopAppCommand

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)
    node = Helpers.parse_node get_rabbit_hostname

    :rabbit_misc.rpc_call(node, :rabbit, :start, [])

    on_exit([], fn ->
      :rabbit_misc.rpc_call(node, :rabbit, :start, [])
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname}}
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate(["extra"], context[:opts]) == {:validation_failure, :too_many_args}
  end

  test "run: request to an active node succeeds", context do
    node = Helpers.parse_node context[:node]
    assert :rabbit_misc.rpc_call(node, :rabbit, :is_running, [])
    assert @command.run([], context[:opts])
    refute :rabbit_misc.rpc_call(node, :rabbit, :is_running, [])
  end

  test "run: request to a non-existent node returns nodedown" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target}
    assert match?({:badrpc, :nodedown}, @command.run([], opts))
  end

  test "banner", context do
    assert @command.banner([], context[:opts]) =~ ~r/Stopping node #{get_rabbit_hostname}/
  end
end
