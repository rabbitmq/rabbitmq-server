## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule EnvironmentCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.EnvironmentCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "validate: argument count validates" do
    assert @command.validate([], %{}) == :ok
    assert @command.validate(["extra"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag target: get_rabbit_hostname()
  test "run: environment request on a named, active RMQ node is successful", context do
    assert @command.run([], context[:opts])[:kernel] != nil
    assert @command.run([], context[:opts])[:rabbit] != nil
  end

  test "run: environment request on nonexistent RabbitMQ node returns a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "banner", context do
    assert @command.banner([], context[:opts])
      =~ ~r/Application environment of node #{get_rabbit_hostname()}/
  end
end
