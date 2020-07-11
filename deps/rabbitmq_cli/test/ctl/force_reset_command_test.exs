## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ForceResetCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ForceResetCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    start_rabbitmq_app()

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate(["extra"], context[:opts]) == {:validation_failure, :too_many_args}
  end

  test "run: force reset request to an active node with a stopped rabbit app succeeds", context do
    add_vhost "some_vhost"
    # ensure the vhost really does exist
    assert vhost_exists? "some_vhost"
    stop_rabbitmq_app()
    assert :ok == @command.run([], context[:opts])
    start_rabbitmq_app()
    # check that the created vhost no longer exists
    assert match?([_], list_vhosts())
  end

  test "run: reset request to an active node with a running rabbit app fails", context do
    add_vhost "some_vhost"
    assert vhost_exists? "some_vhost"
    assert match?({:error, :mnesia_unexpectedly_running}, @command.run([], context[:opts]))
    assert vhost_exists? "some_vhost"
  end

  test "run: request to a non-existent node returns a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "banner", context do
    assert @command.banner([], context[:opts]) =~ ~r/Forcefully resetting node #{get_rabbit_hostname()}/
  end

  test "output mnesia is running error", context do
    exit_code = RabbitMQ.CLI.Core.ExitCodes.exit_software
    assert match?({:error, ^exit_code,
                   "Mnesia is still running on node " <> _},
                   @command.output({:error, :mnesia_unexpectedly_running}, context[:opts]))

  end
end
