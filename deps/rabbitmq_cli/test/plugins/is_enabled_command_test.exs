## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule PluginIsEnabledCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Plugins.Commands.IsEnabledCommand

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
             rabbitmq_home: rabbitmq_home,
             online: false, offline: false}

    on_exit(fn ->
      set_enabled_plugins(enabled_plugins, :online, get_rabbit_hostname(), opts)
    end)


    {:ok, opts: opts}
  end

  setup context do
    {
      :ok,
      opts: Map.merge(context[:opts], %{
              node: get_rabbit_hostname(),
              timeout: 1000
            })
    }
  end



  test "validate: specifying both --online and --offline is reported as invalid", context do
    assert match?(
      {:validation_failure, {:bad_argument, _}},
      @command.validate(["rabbitmq_stomp"], Map.merge(context[:opts], %{online: true, offline: true}))
    )
  end

  test "validate: not specifying any plugins to check is reported as invalid", context do
    opts = context[:opts] |> Map.merge(%{online: true, offline: false})
    assert match?({:validation_failure, :not_enough_args}, @command.validate([], opts))
  end

  test "validate_execution_environment: specifying a non-existent enabled_plugins_file is fine", context do
    assert @command.validate_execution_environment(["rabbitmq_stomp"],
      Map.merge(context[:opts], %{online: false,
                                  offline: true,
                                  enabled_plugins_file: "none"})) == :ok
  end

  test "validate_execution_environment: specifying a non-existent plugins_dir is reported as an error", context do
    opts = context[:opts] |> Map.merge(%{online: false,
                                         offline: true,
                                         plugins_dir: "none"})

    assert @command.validate_execution_environment(["rabbitmq_stomp"], opts) ==
      {:validation_failure, :plugins_dir_does_not_exist}
  end

  test "run: when given a single enabled plugin, reports it as such", context do
    opts = context[:opts] |> Map.merge(%{online: true, offline: false})
    assert match?({:ok, _},
      assert @command.run(["rabbitmq_stomp"], opts))
  end

  test "run: when given a list of actually enabled plugins, reports them as such", context do
    opts = context[:opts] |> Map.merge(%{online: true, offline: false})
    assert match?({:ok, _},
      assert @command.run(["rabbitmq_stomp", "rabbitmq_federation"], opts))
  end

  test "run: when given a list of non-existent plugins, reports them as not enabled", context do
    opts = context[:opts] |> Map.merge(%{online: true, offline: false})
    assert match?({:error, _},
      assert @command.run(["rabbitmq_xyz", "abc_xyz"], opts))
  end

  test "run: when given a list with one non-existent plugin, reports the group as not [all] enabled", context do
    opts = context[:opts] |> Map.merge(%{online: true, offline: false})
    assert match?({:error, _},
      assert @command.run(["rabbitmq_stomp", "abc_xyz"], opts))
  end
end
