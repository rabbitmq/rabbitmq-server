## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule DeleteVhostCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.DeleteVhostCommand
  @vhost "test"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    add_vhost(context[:vhost])
    on_exit(context, fn -> delete_vhost(context[:vhost]) end)

    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "validate: argument count validates" do
    assert @command.validate(["tst"], %{}) == :ok
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["test", "extra"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag vhost: @vhost
  test "run: A valid name to an active RabbitMQ node is successful", context do
    assert @command.run([context[:vhost]], context[:opts]) == :ok

    assert list_vhosts() |> Enum.count(fn(record) -> record[:name] == context[:vhost] end) == 0
  end

  @tag vhost: ""
  test "run: An empty string to an active RabbitMQ node is successful", context do
    assert @command.run([context[:vhost]], context[:opts]) == :ok

    assert list_vhosts() |> Enum.count(fn(record) -> record[:name] == context[:vhost] end) == 0
  end

  test "run: A call to invalid or inactive RabbitMQ node returns a nodedown" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run(["na"], opts))
  end

  @tag vhost: @vhost
  test "run: Deleting the same host twice results in a host not found message", context do
    @command.run([context[:vhost]], context[:opts])
    assert @command.run([context[:vhost]], context[:opts]) ==
      {:error, {:no_such_vhost, context[:vhost]}}
  end

  @tag vhost: @vhost
  test "banner", context do
    s = @command.banner([context[:vhost]], context[:opts])
    assert s =~ ~r/Deleting vhost/
    assert s =~ ~r/\"#{context[:vhost]}\"/
  end
end
