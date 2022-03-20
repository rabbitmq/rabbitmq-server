## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule CloseAllUserConnectionsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.CloseAllUserConnectionsCommand

  @vhost "/"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    close_all_connections(get_rabbit_hostname())

    on_exit([], fn ->
      close_all_connections(get_rabbit_hostname())
    end)

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 30000
      }}
  end

  test "validate: with an invalid number of arguments returns an arg count error", context do
    assert @command.validate(["username", "explanation", "extra"], context[:opts]) ==
             {:validation_failure, :too_many_args}

    assert @command.validate(["username"], context[:opts]) ==
             {:validation_failure, :not_enough_args}
  end

  test "validate: with the correct number of arguments returns ok", context do
    assert @command.validate(["username", "test"], context[:opts]) == :ok
  end

  test "run: a close connections request on a user with open connections", context do
    Application.ensure_all_started(:amqp)
    # open a localhost connection with default username
    {:ok, _conn} = AMQP.Connection.open(virtual_host: @vhost)
    
    await_condition(fn ->
      conns = fetch_user_connections("guest", context)
      length(conns) > 0
    end, 10000)

    # make sure there is a connection to close
    conns = fetch_user_connections("guest", context)
    assert length(conns) > 0

    # make sure closing yeti's connections doesn't affect guest's connections
    assert :ok == @command.run(["yeti", "test"], context[:opts])
    Process.sleep(500)
    conns = fetch_user_connections("guest", context)
    assert length(conns) > 0

    # finally, make sure we can close guest's connections
    assert :ok == @command.run(["guest", "test"], context[:opts])
    await_condition(fn ->
      conns = fetch_user_connections("guest", context)
      length(conns) == 0
    end, 10000)

    conns = fetch_user_connections("guest", context)
    assert length(conns) == 0
  end

  test "run: a close connections request on for a non existing user returns successfully", context do
    assert match?(
             :ok,
             @command.run(["yeti", "test"], context[:opts])
           )
  end

  test "banner", context do
    s = @command.banner(["username", "some reason"], context[:opts])
    assert s =~ ~r/Closing connections/
    assert s =~ ~r/user username/
    assert s =~ ~r/reason: some reason/
  end
end
