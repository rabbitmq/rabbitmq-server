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
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.

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

  test "validate_execution_environment: not specifying an enabled_plugins_file is reported as an error", context do
    opts = context[:opts] |> Map.merge(%{online: false,
                                         offline: true}) |> Map.delete(:enabled_plugins_file)
    assert @command.validate_execution_environment(["rabbitmq_stomp"], opts) ==
      {:validation_failure, :no_plugins_file}
  end

  test "validate_execution_environment: not specifying a plugins_dir is reported as an error", context do
    opts = context[:opts] |> Map.merge(%{online: false,
                                         offline: true}) |> Map.delete(:plugins_dir)
    assert @command.validate_execution_environment(["rabbitmq_stomp"], opts) ==
      {:validation_failure, :no_plugins_dir}
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

  test "validate: failure to load the rabbit application is reported as an error", context do
    opts = context[:opts] |> Map.merge(%{online: false,
                                         offline: true}) |> Map.delete(:rabbitmq_home)
    assert {:validation_failure, {:unable_to_load_rabbit, :rabbitmq_home_is_undefined}} ==
      @command.validate_execution_environment(["rabbitmq_stomp"], opts)
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
