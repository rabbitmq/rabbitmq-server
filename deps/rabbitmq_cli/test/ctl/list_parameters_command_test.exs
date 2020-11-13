## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ListParametersCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListParametersCommand

  @vhost "test1"
  @root   "/"
  @component_name "federation-upstream"
  @key "reconnect-delay"
  @value "{\"uri\":\"amqp://\"}"
  @default_options %{vhost: "/", table_headers: true}

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    node = get_rabbit_hostname()

    {:ok, plugins_file} = :rabbit_misc.rpc_call(node,
                                                :application, :get_env,
                                                [:rabbit, :enabled_plugins_file])
    {:ok, plugins_dir} = :rabbit_misc.rpc_call(node,
                                               :application, :get_env,
                                               [:rabbit, :plugins_dir])
    rabbitmq_home = :rabbit_misc.rpc_call(node, :code, :lib_dir, [:rabbit])

    {:ok, [enabled_plugins]} = :file.consult(plugins_file)

    opts = %{enabled_plugins_file: plugins_file,
             plugins_dir: plugins_dir,
             rabbitmq_home: rabbitmq_home}

    set_enabled_plugins([:rabbitmq_stomp, :rabbitmq_federation], :online, node, opts)

    add_vhost @vhost

    enable_federation_plugin()

    on_exit(fn ->
      set_enabled_plugins(enabled_plugins, :online, get_rabbit_hostname(), opts)
      delete_vhost @vhost
    end)

    :ok
  end

  setup context do
    on_exit(fn ->
      clear_parameter context[:vhost], context[:component_name], context[:key]
    end)
    {
      :ok,
      opts: %{
        node: get_rabbit_hostname(),
        timeout: (context[:timeout] || :infinity),
        vhost: context[:vhost]
      }
    }
  end

  test "merge_defaults: defaults can be overridden" do
    assert @command.merge_defaults([], %{}) == {[], @default_options}
    assert @command.merge_defaults([], %{vhost: "non_default"}) == {[], %{vhost: "non_default",
                                                                          table_headers: true}}
  end

  test "validate: wrong number of arguments leads to an arg count error" do
    assert @command.validate(["this", "is", "too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @vhost
  test "run: a well-formed, host-specific command returns list of parameters", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})
    set_parameter(context[:vhost], context[:component_name], context[:key], @value)
    @command.run([], vhost_opts)
    |> assert_parameter_list(context)
  end

  test "run: throws a badrpc when instructed to contact an unreachable RabbitMQ node" do
    opts = %{node: :jake@thedog, vhost: @vhost, timeout: 200}

    assert match?({:badrpc, _}, @command.run([], opts))
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @root
  test "run: a well-formed command with no vhost runs against the default", context do

    set_parameter("/", context[:component_name], context[:key], @value)
    on_exit(fn ->
      clear_parameter("/", context[:component_name], context[:key])
    end)

    @command.run([], context[:opts])
    |> assert_parameter_list(context)
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @vhost
  test "run: zero timeout return badrpc", context do
    set_parameter(context[:vhost], context[:component_name], context[:key], @value)
    assert @command.run([], Map.put(context[:opts], :timeout, 0)) == {:badrpc, :timeout}
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: "bad-vhost"
  test "run: an invalid vhost returns a no-such-vhost error", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [],
      vhost_opts
    ) == {:error, {:no_such_vhost, context[:vhost]}}
  end

  @tag vhost: @vhost
  test "run: multiple parameters returned in list", context do
    parameters = [
      %{component: "federation-upstream", name: "my-upstream", value: "{\"uri\":\"amqp://\"}"},
      %{component: "exchange-delete-in-progress", name: "my-key", value: "{\"foo\":\"bar\"}"}
    ]
    parameters
    |> Enum.map(
        fn(%{component: component, name: name, value: value}) ->
          set_parameter(context[:vhost], component, name, value)
          on_exit(fn ->
            clear_parameter(context[:vhost], component, name)
          end)
        end)

    params = for param <- @command.run([], context[:opts]), do: Map.new(param)

    assert MapSet.new(params) == MapSet.new(parameters)
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @vhost
  test "banner", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.banner([], vhost_opts)
      =~ ~r/Listing runtime parameters for vhost \"#{context[:vhost]}\" \.\.\./
  end

  # Checks each element of the first parameter against the expected context values
  defp assert_parameter_list(params, context) do
    [param] = params
    assert MapSet.new(param) == MapSet.new([component: context[:component_name],
                                            name: context[:key],
                                            value: context[:value]])
  end
end
