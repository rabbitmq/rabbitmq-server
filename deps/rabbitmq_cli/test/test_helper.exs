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


ExUnit.start()

defmodule TestHelper do
  import ExUnit.Assertions
  alias RabbitMQ.CLI.Plugins.Helpers, as: PluginHelpers

  def get_rabbit_hostname() do
    name = RabbitMQ.CLI.Core.Config.get_option(:nodename)
    name <> "@" <> hostname() |> String.to_atom()
  end

  def hostname() do
    elem(:inet.gethostname, 1) |> List.to_string()
  end

  def get_cluster_name() do
    :rpc.call(get_rabbit_hostname, :rabbit_runtime_parameters, :value_global, [:cluster_name])
  end

  def add_vhost(name) do
    :rpc.call(get_rabbit_hostname, :rabbit_vhost, :add, [name])
  end

  def delete_vhost(name) do
    :rpc.call(get_rabbit_hostname, :rabbit_vhost, :delete, [name])
  end

  def list_vhosts() do
    :rpc.call(get_rabbit_hostname, :rabbit_vhost, :info_all, [])
  end

  def add_user(name, password) do
    :rpc.call(get_rabbit_hostname, :rabbit_auth_backend_internal, :add_user, [name, password])
  end

  def delete_user(name) do
    :rpc.call(get_rabbit_hostname, :rabbit_auth_backend_internal, :delete_user, [name])
  end

  def list_users() do
    :rpc.call(get_rabbit_hostname, :rabbit_auth_backend_internal, :list_users, [])
  end

  def trace_on(vhost) do
    :rpc.call(get_rabbit_hostname, :rabbit_trace, :start, [vhost])
  end

  def trace_off(vhost) do
    :rpc.call(get_rabbit_hostname, :rabbit_trace, :stop, [vhost])
  end

  def set_user_tags(name, tags) do
    :rpc.call(get_rabbit_hostname, :rabbit_auth_backend_internal, :set_tags, [name, tags])
  end

  def authenticate_user(name, password) do
    :rpc.call(get_rabbit_hostname, :rabbit_access_control,:check_user_pass_login, [name, password])
  end

  def set_parameter(vhost, component_name, key, value) do
    :ok = :rpc.call(get_rabbit_hostname, :rabbit_runtime_parameters, :parse_set, [vhost, component_name, key, value, :nouser])
  end

  def clear_parameter(vhost, component_name, key) do
    :rpc.call(get_rabbit_hostname, :rabbit_runtime_parameters, :clear, [vhost, component_name, key])
  end

  def list_parameters(vhost) do
    :rpc.call(get_rabbit_hostname, :rabbit_runtime_parameters, :list_formatted, [vhost])
  end

  def set_permissions(user, vhost, [conf, write, read]) do
    :rpc.call(get_rabbit_hostname, :rabbit_auth_backend_internal, :set_permissions, [user, vhost, conf, write, read])
  end

  def list_policies(vhost) do
    :rpc.call(get_rabbit_hostname, :rabbit_policy, :list_formatted, [vhost])
  end

  def set_policy(vhost, name, pattern, value) do
    {:ok, decoded} = :rabbit_json.try_decode(value)
    parsed = :maps.to_list(decoded)
    :ok = :rpc.call(get_rabbit_hostname, :rabbit_policy, :set, [vhost, name, pattern, parsed, 0, "all"])
  end

  def clear_policy(vhost, key) do
    :rpc.call(get_rabbit_hostname, :rabbit_policy, :delete, [vhost, key])
  end

  def list_operator_policies(vhost) do
    :rpc.call(get_rabbit_hostname, :rabbit_policy, :list_formatted_op, [vhost])
  end

  def set_operator_policy(vhost, name, pattern, value) do
    {:ok, decoded} = :rabbit_json.try_decode(value)
    parsed = :maps.to_list(decoded)
    :ok = :rpc.call(get_rabbit_hostname, :rabbit_policy, :set_op, [vhost, name, pattern, parsed, 0, "all"])
  end

  def clear_operator_policy(vhost, key) do
    :rpc.call(get_rabbit_hostname, :rabbit_policy, :delete_op, [vhost, key])
  end

  def declare_queue(name, vhost, durable \\ false, auto_delete \\ false, args \\ [], owner \\ :none) do
    queue_name = :rabbit_misc.r(vhost, :queue, name)
    :rpc.call(get_rabbit_hostname,
              :rabbit_amqqueue, :declare,
              [queue_name, durable, auto_delete, args, owner])
  end

  def delete_queue(name, vhost) do
    queue_name = :rabbit_misc.r(vhost, :queue, name)
    :rpc.call(get_rabbit_hostname,
              :rabbit_amqqueue, :delete,
              [queue_name, false, false])
  end

  def declare_exchange(name, vhost, type \\ :direct, durable \\ false, auto_delete \\ false, internal \\ false, args \\ []) do
    exchange_name = :rabbit_misc.r(vhost, :exchange, name)
    :rpc.call(get_rabbit_hostname,
              :rabbit_exchange, :declare,
              [exchange_name, type, durable, auto_delete, internal, args])
  end

  def list_permissions(vhost) do
    :rpc.call(
      get_rabbit_hostname,
      :rabbit_auth_backend_internal,
      :list_vhost_permissions,
      [vhost],
      :infinity
    )
  end

  def set_vm_memory_high_watermark(limit) do
    :rpc.call(get_rabbit_hostname, :vm_memory_monitor, :set_vm_memory_high_watermark, [limit])
  end

  def set_disk_free_limit(limit) do
    :rpc.call(get_rabbit_hostname, :rabbit_disk_monitor, :set_disk_free_limit, [limit])
  end

  def start_rabbitmq_app do
    :rabbit_misc.rpc_call(get_rabbit_hostname, :rabbit, :start, [])
    :timer.sleep(1000)
  end

  def stop_rabbitmq_app do
    :rabbit_misc.rpc_call(get_rabbit_hostname, :rabbit, :stop, [])
    :timer.sleep(1000)
  end

  def status do
    :rpc.call(get_rabbit_hostname, :rabbit, :status, [])
  end

  def error_check(cmd_line, code) do
    assert catch_exit(RabbitMQCtl.main(cmd_line)) == {:shutdown, code}
  end

  def with_channel(vhost, fun) do
    with_connection(vhost,
      fn(conn) ->
        {:ok, chan} = AMQP.Channel.open(conn)
        AMQP.Confirm.select(chan)
        fun.(chan)
      end)
  end

  def with_connection(vhost, fun) do
    {:ok, conn} = AMQP.Connection.open(virtual_host: vhost)
    ExUnit.Callbacks.on_exit(fn ->
      try do
        :amqp_connection.close(conn, 1000)
      catch
        :exit, _ -> :ok
      end
    end)
    fun.(conn)
  end

  def message_count(vhost, queue_name) do
    with_channel(vhost, fn(channel) ->
      {:ok, %{message_count: mc}} = AMQP.Queue.declare(channel, queue_name)
      mc
    end)
  end

  def publish_messages(vhost, queue_name, count) do
    with_channel(vhost, fn(channel) ->
      AMQP.Queue.purge(channel, queue_name)
      for i <- 1..count do
        AMQP.Basic.publish(channel, "", queue_name,
                           "test_message" <> Integer.to_string(i))
      end
      AMQP.Confirm.wait_for_confirms(channel, 30)
    end)
  end

  def close_all_connections(node) do
    # we intentionally use connections_local/0 here because connections/0,
    # the cluster-wide version, loads some bits around cluster membership
    # that are not normally ready with a single node. MK.
    #
    # when/if we decide to test
    # this project against a cluster of nodes this will need revisiting. MK.
    for pid <- :rpc.call(node, :rabbit_networking, :connections_local, []) do
      :rpc.call(node, :rabbit_networking, :close_connection, [pid, :force_closed])
    end
  end

  def delete_all_queues() do
    try do
      immediately_delete_all_queues(:rabbit_amqqueue.list())
    catch
      _, _ -> :ok
    end
  end

  def delete_all_queues(vhost) do
    try do
      immediately_delete_all_queues(:rabbit_amqqueue.list(vhost))
    catch
      _, _ -> :ok
    end
  end

  defp immediately_delete_all_queues(qs) do
    for q <- qs do
      try do
        :rpc.call(
          get_rabbit_hostname,
          :rabbit_amqeueue,
          :delete,
          [q, false, false],
          5000
        )
      catch
        _, _ -> :ok
      end
    end
  end

  def reset_vm_memory_high_watermark() do
    try do
      :rpc.call(
        get_rabbit_hostname,
        :vm_memory_monitor,
        :set_vm_memory_high_watermark,
        [0.4],
        5000
      )
    catch
      _, _ -> :ok
    end
  end

  def emit_list_multiple_sources(list1, list2, ref, pid) do
    pids = for list <- [list1, list2], do: Kernel.spawn_link(TestHelper, :emit_list, [list, ref, pid])
    :rabbit_control_misc.await_emitters_termination(pids)
  end

  def emit_list(list, ref, pid) do
    emit_list_map(list, &(&1), ref, pid)
  end

  def emit_list_map(list, fun, ref, pid) do
    :rabbit_control_misc.emitting_map(pid, ref, fun, list)
  end

  def run_command_to_list(command, args) do
    res = Kernel.apply(command, :run, args)
    case Enumerable.impl_for(res) do
      nil -> res;
      _   -> Enum.to_list(res)
    end
  end

  def vhost_exists?(vhost) do
    Enum.any?(list_vhosts, fn(v) -> v[:name] == vhost end)
  end

  def set_enabled_plugins(node, plugins, opts) do
    PluginHelpers.set_enabled_plugins(plugins, :online, node, opts)
  end

  def currently_active_plugins(context) do
    Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))
  end

  def enable_federation_plugin() do
    node = get_rabbit_hostname
    {:ok, plugins_file} = :rabbit_misc.rpc_call(node,
                                                :application, :get_env,
                                                [:rabbit, :enabled_plugins_file])
    {:ok, plugins_dir} = :rabbit_misc.rpc_call(node,
                                               :application, :get_env,
                                               [:rabbit, :plugins_dir])
    rabbitmq_home = :rabbit_misc.rpc_call(node, :code, :lib_dir, [:rabbit])
    {:ok, [_enabled_plugins]} = :file.consult(plugins_file)

    opts = %{enabled_plugins_file: plugins_file,
             plugins_dir: plugins_dir,
             rabbitmq_home: rabbitmq_home,
             online: true, offline: false}

    plugins = currently_active_plugins(%{opts: %{node: node}})
    case Enum.member?(plugins, :rabbitmq_federation) do
      true  -> :ok
      false ->
        set_enabled_plugins(get_rabbit_hostname, plugins ++ [:rabbitmq_federation], opts)
    end
  end

  def set_vhost_limits(vhost, limits) do
    :rpc.call(get_rabbit_hostname,
              :rabbit_vhost_limit, :parse_set, [vhost, limits])
  end
  def get_vhost_limits(vhost) do
    :rpc.call(get_rabbit_hostname, :rabbit_vhost_limit, :list, [vhost])
    |> Map.new
  end

  def clear_vhost_limits(vhost) do
    :rpc.call(get_rabbit_hostname, :rabbit_vhost_limit, :clear, [vhost])
  end
end
