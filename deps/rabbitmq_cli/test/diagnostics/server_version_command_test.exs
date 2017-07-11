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


defmodule ServerVersionCommandTest do
  use ExUnit.Case
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.ServerVersionCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 30000
      }}
  end

  test "merge_defaults: nothing to do" do
    assert @command.merge_defaults([], %{}) == {[], %{}}
  end

  test "validate: treats positional arguments as a failure" do
    assert @command.validate(["extra-arg"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: treats empty positional arguments and default switches as a success" do
    assert @command.validate([], %{}) == :ok
  end

  @tag test_timeout: 0
  test "run: targeting an unreachable node throws a badrpc", context do
    target = :jake@thedog

    opts = %{node: target}
    assert @command.run([], Map.merge(context[:opts], opts)) == {:badrpc, :nodedown}
  end

  test "run: returns RabbitMQ version on the target node", context do
    res = @command.run([], context[:opts])
    # assert that we have a list of characters
    assert length(res) > 0 and :io_lib.char_list(res)
  end
end
