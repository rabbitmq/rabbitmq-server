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
