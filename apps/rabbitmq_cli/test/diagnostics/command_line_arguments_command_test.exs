## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule CommandLineArgumentsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.CommandLineArgumentsCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname(), timeout: :infinity}}
  end

  test "validate: with extra arguments, command line arguments returns an arg count error", context do
    assert @command.validate(["extra"], context[:opts]) ==
    {:validation_failure, :too_many_args}
  end

  test "run: command line arguments request to a reachable node succeeds", context do
    output = @command.run([], context[:opts]) |> Enum.to_list

    assert_stream_without_errors(output)
  end

  test "run: command line arguments request on nonexistent RabbitMQ node returns a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "banner", context do
    assert @command.banner([], context[:opts])
      =~ ~r/Command line arguments of node #{get_rabbit_hostname()}/
  end
end
