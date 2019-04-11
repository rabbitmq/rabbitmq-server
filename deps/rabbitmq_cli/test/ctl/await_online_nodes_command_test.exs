## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.


defmodule AwaitOnlineNodesCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.AwaitOnlineNodesCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()


    {:ok, opts: %{node: get_rabbit_hostname(), timeout: 300_000}}
  end

  setup context do
    on_exit(context, fn -> delete_vhost(context[:vhost]) end)
    :ok
  end

  test "validate: wrong number of arguments results in arg count errors" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["1", "1"], %{}) == {:validation_failure, :too_many_args}
  end

  test "run: a call with node count of 1 with a running RabbitMQ node succeeds", context do
    assert @command.run(["1"], context[:opts]) == :ok
  end

  test "run: a call to an unreachable RabbitMQ node returns a nodedown" do
    opts   = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run(["1"], opts))
  end

  test "banner", context do
    assert @command.banner(["1"], context[:opts])
      =~ ~r/Will wait for at least 1 nodes to join the cluster of #{context[:opts][:node]}. Timeout: 300 seconds./
  end

end
