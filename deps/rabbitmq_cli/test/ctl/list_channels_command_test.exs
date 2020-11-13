## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule ListChannelsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListChannelsCommand
  @default_timeout :infinity

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    close_all_connections(get_rabbit_hostname())

    on_exit([], fn ->
      close_all_connections(get_rabbit_hostname())
    end)

    :ok
  end

  setup context do
    {
      :ok,
      opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || @default_timeout
      }
    }
  end

  test "merge_defaults: default channel info keys are pid, user, consumer_count, and messages_unacknowledged", context do
    assert match?({~w(pid user consumer_count messages_unacknowledged), _}, @command.merge_defaults([], context[:opts]))
  end

  test "validate: returns bad_info_key on a single bad arg", context do
    assert @command.validate(["quack"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:quack]}}
  end

  test "validate: returns multiple bad args return a list of bad info key values", context do
    assert @command.validate(["quack", "oink"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink, :quack]}}
  end

  test "validate: returns bad_info_key on mix of good and bad args", context do
    assert @command.validate(["quack", "pid"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:quack]}}
    assert @command.validate(["user", "oink"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink]}}
    assert @command.validate(["user", "oink", "pid"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink]}}
  end

  @tag test_timeout: 0
  test "run: zero timeout causes command to return badrpc", context do
    assert run_command_to_list(@command, [["user"], context[:opts]]) ==
      [{:badrpc, {:timeout, 0.0}}]
  end

  test "run: multiple channels on multiple connections", context do
    node_name = get_rabbit_hostname()
    close_all_connections(node_name)
    existent_channels = :rabbit_misc.rpc_call(node_name,:rabbit_channel, :list, [])
    with_channel("/", fn(_channel1) ->
      with_channel("/", fn(_channel2) ->
        all_channels = run_command_to_list(@command, [["pid", "user", "connection"], context[:opts]])
        channels = Enum.filter(all_channels,
                               fn(ch) ->
                                 not Enum.member?(existent_channels, ch[:pid])
                               end)
        chan1 = Enum.at(channels, 0)
        chan2 = Enum.at(channels, 1)
        assert Keyword.keys(chan1) == ~w(pid user connection)a
        assert Keyword.keys(chan2) == ~w(pid user connection)a
        assert "guest" == chan1[:user]
        assert "guest" == chan2[:user]
        assert chan1[:pid] !== chan2[:pid]
      end)
    end)
  end

  test "run: multiple channels on single connection", context do
    node_name = get_rabbit_hostname()
    close_all_connections(get_rabbit_hostname())
    with_connection("/", fn(conn) ->
      existent_channels = :rabbit_misc.rpc_call(node_name,:rabbit_channel, :list, [])
      {:ok, _} = AMQP.Channel.open(conn)
      {:ok, _} = AMQP.Channel.open(conn)
      all_channels = run_command_to_list(@command, [["pid", "user", "connection"], context[:opts]])
      channels = Enum.filter(all_channels,
                             fn(ch) ->
                               not Enum.member?(existent_channels, ch[:pid])
                             end)

      chan1 = Enum.at(channels, 0)
      chan2 = Enum.at(channels, 1)
      assert Keyword.keys(chan1) == ~w(pid user connection)a
      assert Keyword.keys(chan2) == ~w(pid user connection)a
      assert "guest" == chan1[:user]
      assert "guest" == chan2[:user]
      assert chan1[:pid] !== chan2[:pid]
    end)
  end

  test "run: info keys order is preserved", context do
    close_all_connections(get_rabbit_hostname())
    with_channel("/", fn(_channel) ->
      channels = run_command_to_list(@command, [~w(connection vhost name pid number user), context[:opts]])
      chan     = Enum.at(channels, 0)
      assert Keyword.keys(chan) == ~w(connection vhost name pid number user)a
    end)
  end
end
