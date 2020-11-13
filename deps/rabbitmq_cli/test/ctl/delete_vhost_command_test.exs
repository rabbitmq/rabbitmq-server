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
