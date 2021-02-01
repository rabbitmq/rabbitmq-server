## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule CloseAllUserConnectionsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  # alias RabbitMQ.CLI.Ctl.RpcStream

  @helpers RabbitMQ.CLI.Core.Helpers

  @command RabbitMQ.CLI.Ctl.Commands.CloseAllUserConnectionsCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    close_all_connections(get_rabbit_hostname())

    on_exit([], fn ->
      close_all_connections(get_rabbit_hostname())
    end)

    :ok
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
    with_connection("/", fn _ ->
      node = @helpers.normalise_node(context[:node], :shortnames)
      Process.sleep(500)

      # make sure there is a connection to close
      conns = fetch_user_connections("guest", context)
      assert length(conns) > 0

      # make sure closing yeti's connections doesn't affect guest's connections
      assert :ok == @command.run(["yeti", "test"], %{node: node})
      Process.sleep(500)
      conns = fetch_user_connections("guest", context)
      assert length(conns) > 0

      # finally, make sure we can close guest's connections
      assert :ok == @command.run(["guest", "test"], %{node: node})
      Process.sleep(500)
      conns = fetch_user_connections("guest", context)
      assert length(conns) == 0
    end)
  end

  test "run: a close connections request on for a non existing user returns successfully",
       context do
    assert match?(
             :ok,
             @command.run(["yeti", "test"], %{
               node: @helpers.normalise_node(context[:node], :shortnames)
             })
           )
  end

  test "banner", context do
    s = @command.banner(["username", "some reason"], context[:opts])
    assert s =~ ~r/Closing connections/
    assert s =~ ~r/user username/
    assert s =~ ~r/reason: some reason/
  end

  defp fetch_user_connections(username, context) do
    node = @helpers.normalise_node(context[:node], :shortnames)

    :rabbit_misc.rpc_call(node, :rabbit_connection_tracking, :list_of_user, [
      username
    ])
  end
end
