## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.


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
  test "run: a valid name to an active RabbitMQ node is successful", context do
    assert @command.run([context[:vhost]], context[:opts]) == :ok
    assert list_vhosts() |> Enum.count(fn(record) -> record[:name] == context[:vhost] end) == 1
  end

  @tag vhost: ""
  test "run: An empty string to an active RabbitMQ node is still successful", context do
    assert @command.run([context[:vhost]], context[:opts]) == :ok
    assert list_vhosts() |> Enum.count(fn(record) -> record[:name] == context[:vhost] end) == 1
  end

  test "run: A call to invalid or inactive RabbitMQ node returns a nodedown" do
    target = :jake@thedog

    opts = %{node: target}
    assert @command.run(["na"], opts) == {:badrpc, :nodedown}
  end

  test "run: Adding the same host twice results in a host exists message", context do
    add_vhost context[:vhost]

    assert @command.run([context[:vhost]], context[:opts]) ==
        {:error, {:vhost_already_exists, context[:vhost]}}

    assert list_vhosts() |> Enum.count(fn(record) -> record[:name] == context[:vhost] end) == 1
  end

  @tag vhost: @vhost
  test "banner", context do
    assert @command.banner([context[:vhost]], context[:opts])
      =~ ~r/Adding vhost \"#{context[:vhost]}\" \.\.\./
  end

end
