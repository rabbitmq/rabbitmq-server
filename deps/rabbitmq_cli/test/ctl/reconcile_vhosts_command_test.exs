## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule ReconcileVhostsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ReconcileVhostsCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    :ok
  end

  @vhost "vhost_to_reconcile"
  @timeout 10000

  setup do
    {:ok,
     opts: %{
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
             @command.run([], opts)
           )
  end

  test "banner", context do
    assert @command.banner([], context[:opts]) =~
             ~r/Will try to initiate virtual host reconciliation/
  end

  test "run: initiates an async operation and returns ok", context do
    setup_vhosts()
    vhost = context[:opts][:vhost]
    node_name = context[:opts][:node]
    force_vhost_failure(node_name, vhost)
    assert :ok == @command.run([], context[:opts])
    :timer.sleep(1000)
    assert match?({:ok, _}, :rpc.call(node_name, :rabbit_vhost_sup_sup, :get_vhost_sup, [vhost]))
  end

  #
  # Implementation
  #

  defp setup_vhosts do
    add_vhost(@vhost)
    # give the vhost a chance to fully start and initialise
    :timer.sleep(1000)

    on_exit(fn ->
      delete_vhost(@vhost)
    end)
  end

  defp force_vhost_failure(node_name, vhost) do
    case :rpc.call(node_name, :rabbit_vhost_sup_sup, :get_vhost_sup, [vhost]) do
      {:ok, sup} ->
        case :lists.keyfind(:msg_store_persistent, 1, :supervisor.which_children(sup)) do
          {_, pid, _, _} ->
            Process.exit(pid, :foo)
            :timer.sleep(5000)
            force_vhost_failure(node_name, vhost)

          false ->
            Process.exit(sup, :foo)
            :timer.sleep(5000)
            force_vhost_failure(node_name, vhost)
        end

      {:error, {:vhost_supervisor_not_running, _}} ->
        :ok
    end
  end
end
