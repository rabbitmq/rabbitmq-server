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
## Copyright (c) 2016-2017 Pivotal Software, Inc.  All rights reserved.


defmodule RestartVhostCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.RestartVhostCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    :ok
  end

  @vhost "vhost_to_restart"
  @timeout 10000

  setup do
    add_vhost @vhost
    on_exit(fn ->
      delete_vhost @vhost
    end)
    {:ok, opts: %{
      node: get_rabbit_hostname(),
      vhost: @vhost,
      timeout: @timeout
    }}
  end

  test "validate: specifying arguments is reported as an error", context do
    assert @command.validate(["a"], context[:opts]) ==
      {:validation_failure, :too_many_args}
    assert @command.validate(["a", "b"], context[:opts]) ==
      {:validation_failure, :too_many_args}
    assert @command.validate(["a", "b", "c"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  test "run: request to a non-existent node returns nodedown", _context do
    target = :jake@thedog

    opts = %{node: target, vhost: @vhost, timeout: @timeout}
    # We use "self" node as the target. It's enough to trigger the error.
    assert match?(
      {:badrpc, :nodedown},
      @command.run([], opts))
  end

  test "banner", context do
    expected = "Trying to restart vhost '#{@vhost}' on node '#{get_rabbit_hostname()}' ..."
    ^expected = @command.banner([], context[:opts])
  end

  test "run: restarting an existing vhost returns already_started", context do
    {:error, {:already_started, _}} = @command.run([], context[:opts])
  end

  test "run: restarting an failed vhost returns ok", context do
    vhost = context[:opts][:vhost]
    node_name = context[:opts][:node]
    force_vhost_failure(node_name, vhost)
    {:ok, _} = @command.run([], context[:opts])
    {:ok, _} = :rpc.call(node_name, :rabbit_vhost_sup_sup, :get_vhost_sup, [vhost])
  end

  def force_vhost_failure(node_name, vhost) do
    case :rpc.call(node_name, :rabbit_vhost_sup_sup, :get_vhost_sup, [vhost]) do
      {:ok, sup} ->
        {_, pid, _, _} = :lists.keyfind(:msg_store_persistent, 1, :supervisor.which_children(sup))
        Process.exit(pid, :foo)
        :timer.sleep(100)
        force_vhost_failure(node_name, vhost);
      {:error, {:vhost_supervisor_not_running, _}} ->
        :ok
    end
  end

end
