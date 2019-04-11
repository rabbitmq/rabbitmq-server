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
