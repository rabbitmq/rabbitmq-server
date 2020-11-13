## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule AutocompleteCommandTest do
  use ExUnit.Case, async: true
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.AutocompleteCommand
  setup do
    {:ok, opts: %{
      script_name: "rabbitmqctl",
      node: get_rabbit_hostname()
    }}
  end

  test "shows up in help" do
    s = @command.usage()
    assert s =~ ~r/autocomplete/
  end

  test "enforces --silent" do
    assert @command.merge_defaults(["list_"], %{}) == {["list_"], %{silent: true}}
  end

  test "validate: providing no arguments fails validation" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: providing two or more arguments fails validation" do
    assert @command.validate(["list_", "extra"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: providing a single argument passes validation" do
    assert @command.validate(["list_c"], %{}) == :ok
  end

  test "run: lists completion options", context do
    {:stream, completion_options} = @command.run(["list_c"], context[:opts])

    assert Enum.member?(completion_options, "list_channels")
    assert Enum.member?(completion_options, "list_connections")
    assert Enum.member?(completion_options, "list_consumers")
  end

  test "banner shows that the name is being set", context do
    assert @command.banner(["list_"], context[:opts]) == nil
  end
end
