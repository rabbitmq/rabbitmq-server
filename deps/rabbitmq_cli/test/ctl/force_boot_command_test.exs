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


defmodule ForceBootCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ForceBootCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()


    :ok
  end

  setup _ do
    {
      :ok,
     opts: %{
       node: get_rabbit_hostname(),
       timeout: 1000
     }
    }
  end

  test "validate: providing too many arguments fails validation" do
    assert @command.validate(["many"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: the rabbit app running on target node fails validation", context do
    assert @command.validate_execution_environment([], context[:opts]) == {:validation_failure, :rabbit_app_is_running}
  end

  test "run: sets a force boot marker file on target node", context do
    stop_rabbitmq_app()
    on_exit(fn -> start_rabbitmq_app() end)
    assert @command.run([], context[:opts]) == :ok
    mnesia_dir = :rpc.call(get_rabbit_hostname(), :rabbit_mnesia, :dir, [])

    path = Path.join(mnesia_dir, "force_load")
    assert File.exists?(path)
    File.rm(path)
  end

  test "run: if RABBITMQ_MNESIA_DIR is defined, creates a force boot marker file" do
    node = :unknown@localhost
    temp_dir = "#{Mix.Project.config[:elixirc_paths]}/tmp"
    File.mkdir_p!(temp_dir)
    on_exit(fn -> File.rm_rf!(temp_dir) end)
    System.put_env("RABBITMQ_MNESIA_DIR", temp_dir)

    assert @command.run([], %{node: node}) == :ok
    assert File.exists?(Path.join(temp_dir, "force_load"))

    System.delete_env("RABBITMQ_MNESIA_DIR")
    assert @command.run([], %{node: node}) == {:error, :mnesia_dir_not_found}

    File.rm_rf(temp_dir)
  end
end
