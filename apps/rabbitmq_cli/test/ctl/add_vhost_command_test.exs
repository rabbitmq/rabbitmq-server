## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


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

  test "validate: no arguments fails validation" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: too many arguments fails validation" do
    assert @command.validate(["test", "extra"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: one argument passes validation" do
    assert @command.validate(["new-vhost"], %{}) == :ok
    assert @command.validate(["new-vhost"], %{description: "Used by team A"}) == :ok
    assert @command.validate(["new-vhost"], %{description: "Used by team A for QA purposes", tags: "qa,team-a"}) == :ok
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
