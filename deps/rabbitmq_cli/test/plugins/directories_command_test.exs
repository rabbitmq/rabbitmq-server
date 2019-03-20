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

defmodule DirectoriesCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Plugins.Commands.DirectoriesCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    node = get_rabbit_hostname()

    {:ok, plugins_file} = :rabbit_misc.rpc_call(node,
                                                :application, :get_env,
                                                [:rabbit, :enabled_plugins_file])
    {:ok, plugins_dir} = :rabbit_misc.rpc_call(node,
                                               :application, :get_env,
                                               [:rabbit, :plugins_dir])
    {:ok, plugins_expand_dir} = :rabbit_misc.rpc_call(node,
                                               :application, :get_env,
                                               [:rabbit, :plugins_expand_dir])

    rabbitmq_home = :rabbit_misc.rpc_call(node, :code, :lib_dir, [:rabbit])

    {:ok, opts: %{
        plugins_file: plugins_file,
        plugins_dir: plugins_dir,
        plugins_expand_dir: plugins_expand_dir,
        rabbitmq_home: rabbitmq_home,
     }}
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

  test "validate: providing no arguments passes validation", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "validate: providing --online passes validation", context do
    assert @command.validate([], Map.merge(%{online: true}, context[:opts])) == :ok
  end

  test "validate: providing --offline passes validation", context do
    assert @command.validate([], Map.merge(%{offline: true}, context[:opts])) == :ok
  end

  test "validate: providing any arguments fails validation", context do
    assert @command.validate(["a", "b", "c"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  test "validate: setting both --online and --offline to false fails validation", context do
    assert @command.validate([], Map.merge(context[:opts], %{online: false, offline: false})) ==
      {:validation_failure, {:bad_argument, "Cannot set online and offline to false"}}
  end

  test "validate: setting both --online and --offline to true fails validation", context do
    assert @command.validate([], Map.merge(context[:opts], %{online: true, offline: true})) ==
      {:validation_failure, {:bad_argument, "Cannot set both online and offline"}}
  end

  test "validate_execution_environment: when --offline is used, not specifying an enabled_plugins_file fails validation", context do
    opts = context[:opts] |> Map.merge(%{offline: true}) |> Map.delete(:enabled_plugins_file)
    assert @command.validate_execution_environment([], opts) == {:validation_failure, :no_plugins_file}
  end

  test "validate_execution_environment: when --offline is used, not specifying a plugins_dir fails validation", context do
    opts = context[:opts] |> Map.merge(%{offline: true}) |> Map.delete(:plugins_dir)
    assert @command.validate_execution_environment([], opts) == {:validation_failure, :no_plugins_dir}
  end

  test "validate_execution_environment: when --offline is used, specifying a non-existent enabled_plugins_file passes validation", context do
    opts = context[:opts] |> Map.merge(%{offline: true, enabled_plugins_file: "none"})
    assert @command.validate_execution_environment([], opts) == :ok
  end

  test "validate_execution_environment: when --offline is used, specifying a non-existent plugins_dir fails validation", context do
    opts = context[:opts] |> Map.merge(%{offline: true, plugins_dir: "none"})
    assert @command.validate_execution_environment([], opts) == {:validation_failure, :plugins_dir_does_not_exist}
  end

  test "validate_execution_environment: when --online is used, not specifying an enabled_plugins_file passes validation", context do
    opts = context[:opts] |> Map.merge(%{online: true}) |> Map.delete(:enabled_plugins_file)
    assert @command.validate_execution_environment([], opts) == :ok
  end

  test "validate_execution_environment: when --online is used, not specifying a plugins_dir passes validation", context do
    opts = context[:opts] |> Map.merge(%{online: true}) |> Map.delete(:plugins_dir)
    assert @command.validate_execution_environment([], opts) == :ok
  end

  test "validate_execution_environment: when --online is used, specifying a non-existent enabled_plugins_file passes validation", context do
    opts = context[:opts] |> Map.merge(%{online: true, enabled_plugins_file: "none"})
    assert @command.validate_execution_environment([], opts) == :ok
  end

  test "validate_execution_environment: when --online is used, specifying a non-existent plugins_dir passes validation", context do
    opts = context[:opts] |> Map.merge(%{online: true, plugins_dir: "none"})
    assert @command.validate_execution_environment([], opts) == :ok
  end


  test "run: when --online is used, lists plugin directories", context do
    opts = Map.merge(context[:opts], %{online: true})
    dirs = %{plugins_dir: to_string(Map.get(opts, :plugins_dir)),
             plugins_expand_dir: to_string(Map.get(opts, :plugins_expand_dir)),
             enabled_plugins_file: to_string(Map.get(opts, :plugins_file))}

    assert @command.run([], opts) == {:ok, dirs}
  end
end
