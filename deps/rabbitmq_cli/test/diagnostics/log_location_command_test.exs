## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule LogLocationCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.LogLocationCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    start_rabbitmq_app()

    ExUnit.configure([max_cases: 1])

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 30000,
        all: false
      }}
  end

  test "merge_defaults: all is false" do
    assert @command.merge_defaults([], %{}) == {[], %{all: :false}}
  end

  test "validate: treats positional arguments as a failure" do
    assert @command.validate(["extra-arg"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: treats empty positional arguments and default switches as a success" do
    assert @command.validate([], %{all: :false}) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?({:badrpc, _}, @command.run([], Map.merge(context[:opts], %{node: :jake@thedog, timeout: 100})))
  end

  test "run: prints default log location", context do
    # Let Lager's log message rate lapse or else some messages
    # we assert on might be dropped. MK.
    Process.sleep(1000)
    {:ok, logfile} = @command.run([], context[:opts])
    log_message = "file location"
    :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, [log_message])
    wait_for_log_message(log_message, logfile)
    {:ok, log_file_data} = File.read(logfile)
    assert String.match?(log_file_data, Regex.compile!(log_message))
  end

  test "run: shows all log locations", context do
    # Let Lager's log message rate lapse or else some messages
    # we assert on might be dropped. MK.
    Process.sleep(1000)
    # This assumes default configuration
    [logfile, upgrade_log_file] =
      @command.run([], Map.merge(context[:opts], %{all: true}))

    log_message = "checking the default log file when checking all"
    :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, [log_message])
    wait_for_log_message(log_message, logfile)

    log_message_upgrade = "checking the upgrade log file when checking all"
    :rpc.call(get_rabbit_hostname(),
              :rabbit_log, :log, [:upgrade, :error, log_message_upgrade, []])
    wait_for_log_message(log_message_upgrade, upgrade_log_file)
  end

  test "run: fails if there is no log file configured", context do
    {:ok, upgrade_file} = :rpc.call(get_rabbit_hostname(), :application, :get_env, [:rabbit, :lager_upgrade_file])
    {:ok, default_file} = :rpc.call(get_rabbit_hostname(), :application, :get_env, [:rabbit, :lager_default_file])
    on_exit([], fn ->
      :rpc.call(get_rabbit_hostname(), :application, :set_env, [:rabbit, :lager_upgrade_file, upgrade_file])
      :rpc.call(get_rabbit_hostname(), :application, :set_env, [:rabbit, :lager_default_file, default_file])
      :rpc.call(get_rabbit_hostname(), :rabbit_lager, :configure_lager, [])
      start_rabbitmq_app()
    end)
    stop_rabbitmq_app()
    :rpc.call(get_rabbit_hostname(), :application, :unset_env, [:rabbit, :lager_upgrade_file])
    :rpc.call(get_rabbit_hostname(), :application, :unset_env, [:rabbit, :lager_default_file])
    :rpc.call(get_rabbit_hostname(), :application, :unset_env, [:rabbit, :log])
    :rpc.call(get_rabbit_hostname(), :rabbit_lager, :configure_lager, [])
    {:error, "No log files configured on the node"} = @command.run([], context[:opts])
  end
end
