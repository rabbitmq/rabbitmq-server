## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

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
    assert @command.validate_execution_environment([], context[:opts]) ==
             {:validation_failure, :rabbit_app_is_running}
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
    temp_dir = "#{Mix.Project.config()[:elixirc_paths]}/tmp"
    File.mkdir_p!(temp_dir)
    on_exit(fn -> File.rm_rf!(temp_dir) end)
    System.put_env("RABBITMQ_MNESIA_DIR", temp_dir)

    assert @command.run([], %{node: node}) == :ok
    assert File.exists?(Path.join(temp_dir, "force_load"))

    System.delete_env("RABBITMQ_MNESIA_DIR")
    File.rm_rf(temp_dir)
  end
end
