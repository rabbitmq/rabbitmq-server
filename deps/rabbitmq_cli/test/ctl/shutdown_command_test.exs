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


defmodule ShutdownCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ShutdownCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()


    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname(), timeout: 15}}
  end

  test "validate accepts no arguments", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate(["extra"], context[:opts]) == {:validation_failure, :too_many_args}
  end

  test "validate: in wait mode, checks if local and target node hostnames match" do
    assert match?({:validation_failure, {:unsupported_target, _}},
                  @command.validate([], %{wait: true, node: :'rabbit@some.remote.hostname'}))
  end

  test "validate: in wait mode, always assumes @localhost nodes are local" do
    assert @command.validate([], %{wait: true, node: :rabbit@localhost}) == :ok
  end

  test "validate: in no wait mode, passes unconditionally", context do
    assert @command.validate([], Map.merge(%{wait: false}, context[:opts])) == :ok
  end

  test "run: request to a non-existent node returns a badrpc" do
    opts = %{node: :jake@thedog, wait: false, timeout: 200}
    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "empty banner", context do
    nil = @command.banner([], context[:opts])
  end
end
