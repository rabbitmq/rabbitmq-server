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
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.

defmodule DiscoverPeersCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.DiscoverPeersCommand
  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    :net_kernel.connect_node(get_rabbit_hostname())


    on_exit(fn ->
      :erlang.disconnect_node(get_rabbit_hostname())
    end)

    :ok
  end

  setup context do
    {:ok, opts: %{node: get_rabbit_hostname(), timeout: context[:test_timeout]}}
  end

  @tag test_timeout: 15000
  test "run: return an empty list nodes when the Backend is not configured", context do
    assert {:rabbit_peer_discovery_classic_config, []} == @command.run([], context[:opts])
  end
  
end