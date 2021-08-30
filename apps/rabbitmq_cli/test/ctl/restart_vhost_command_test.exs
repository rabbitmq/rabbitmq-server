## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2020 VMware, Inc. or its affiliates.  All rights reserved.


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

  test "run: request to a non-existent node returns a badrpc", _context do
    opts = %{node: :jake@thedog, vhost: @vhost, timeout: @timeout}
    assert match?(
      {:badrpc, _},
      @command.run([], opts))
  end

  test "banner", context do
    expected = "Trying to restart vhost '#{@vhost}' on node '#{get_rabbit_hostname()}' ..."
    ^expected = @command.banner([], context[:opts])
  end

  test "run: restarting an existing vhost returns already_started", context do
    setup_vhosts()
    {:error, {:already_started, _}} = @command.run([], context[:opts])
  end

  test "run: restarting an failed vhost returns ok", context do
    setup_vhosts()
    vhost = context[:opts][:vhost]
    node_name = context[:opts][:node]
    force_vhost_failure(node_name, vhost)
    {:ok, _} = @command.run([], context[:opts])
    {:ok, _} = :rpc.call(node_name, :rabbit_vhost_sup_sup, :get_vhost_sup, [vhost])
  end

  #
  # Implementation
  #

  defp setup_vhosts do
    add_vhost @vhost
    # give the vhost a chance to fully start and initialise
    :timer.sleep(1000)
    on_exit(fn ->
      delete_vhost @vhost
    end)
  end

  defp force_vhost_failure(node_name, vhost) do
    case :rpc.call(node_name, :rabbit_vhost_sup_sup, :get_vhost_sup, [vhost]) do
      {:ok, sup} ->
        case :lists.keyfind(:msg_store_persistent, 1, :supervisor.which_children(sup)) do
        {_, pid, _, _} ->
          Process.exit(pid, :foo)
          :timer.sleep(5000)
          force_vhost_failure(node_name, vhost);
        false ->
          Process.exit(sup, :foo)
          :timer.sleep(5000)
          force_vhost_failure(node_name, vhost)
        end;
      {:error, {:vhost_supervisor_not_running, _}} ->
        :ok
    end
  end
end
