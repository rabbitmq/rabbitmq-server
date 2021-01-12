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
    {:ok, logfile} = @command.run([], context[:opts])
    log_message = "file location"
    :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, [to_charlist(log_message)])
    wait_for_log_message(log_message, logfile)
    {:ok, log_file_data} = File.read(logfile)
    assert String.match?(log_file_data, Regex.compile!(log_message))
  end

  test "run: shows all log locations", context do
    # This assumes default configuration
    [logfile, upgrade_log_file | _] =
      @command.run([], Map.merge(context[:opts], %{all: true}))

    log_message = "checking the default log file when checking all"
    :rpc.call(get_rabbit_hostname(), :rabbit_log, :error, [to_charlist(log_message)])
    wait_for_log_message(log_message, logfile)

    log_message_upgrade = "checking the upgrade log file when checking all"
    :rpc.call(get_rabbit_hostname(),
              :rabbit_log, :log, [:upgrade, :error, to_charlist(log_message_upgrade), []])
    wait_for_log_message(log_message_upgrade, upgrade_log_file)
  end
end
