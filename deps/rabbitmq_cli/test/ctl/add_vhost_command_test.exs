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


defmodule AddVhostCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.AddVhostCommand
  @vhost "test"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  setup context do
    on_exit(context, fn -> delete_vhost(context[:vhost]) end)
    :ok
  end

  test "validate: wrong number of arguments results in arg count errors" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["test", "extra"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag vhost: @vhost
  test "run: passing a valid vhost name to a running RabbitMQ node succeeds", context do
    assert @command.run([context[:vhost]], context[:opts]) == :ok
    assert list_vhosts() |> Enum.count(fn(record) -> record[:name] == context[:vhost] end) == 1
  end

  @tag vhost: ""
  test "run: passing an empty string for vhost name with a running RabbitMQ node still succeeds", context do
    assert @command.run([context[:vhost]], context[:opts]) == :ok
    assert list_vhosts() |> Enum.count(fn(record) -> record[:name] == context[:vhost] end) == 1
  end

  test "run: attempt to use an unreachable node returns a nodedown" do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run(["na"], opts))
  end

  test "run: adding the same host twice is idempotent", context do
    add_vhost context[:vhost]

    assert @command.run([context[:vhost]], context[:opts]) == :ok
    assert list_vhosts() |> Enum.count(fn(record) -> record[:name] == context[:vhost] end) == 1
  end

  @tag vhost: @vhost
  test "banner", context do
    assert @command.banner([context[:vhost]], context[:opts])
      =~ ~r/Adding vhost \"#{context[:vhost]}\" \.\.\./
  end
end
