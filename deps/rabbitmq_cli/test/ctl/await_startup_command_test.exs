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


defmodule AwaitStartupCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.AwaitStartupCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    {:ok, opts: %{node: get_rabbit_hostname(), timeout: 300_000}}
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "merge_defaults: default timeout is 5 minutes" do
    assert @command.merge_defaults([], %{}) == {[], %{timeout: 300}}
  end

  test "validate: accepts no arguments", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate(["extra"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  test "run: request to a non-existent node returns a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "run: request to a fully booted node succeeds", context do
    assert @command.run([], Map.merge(context[:opts], %{timeout: 5})) == :ok
  end

  test "empty banner", context do
    nil = @command.banner([], context[:opts])
  end
end
