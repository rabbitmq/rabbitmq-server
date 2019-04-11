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

defmodule CheckPortConnectivityCommandTest do
  use ExUnit.Case
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.CheckPortConnectivityCommand

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

  test "merge_defaults: provides a default timeout" do
    assert @command.merge_defaults([], %{}) == {[], %{timeout: 30000}}
  end

  test "validate: treats positional arguments as a failure" do
    assert @command.validate(["extra-arg"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: treats empty positional arguments and default switches as a success" do
    assert @command.validate([], %{}) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?({:badrpc, _}, @command.run([], Map.merge(context[:opts], %{node: :jake@thedog})))
  end

  test "run: tries to connect to every inferred active listener", context do
    assert match?({true, _}, @command.run([], context[:opts]))
  end


  test "output: when all connections succeeded, returns a success", context do
    assert match?({:ok, _}, @command.output({true, []}, context[:opts]))
  end

  # note: it's run/2 that filters out non-local alarms
  test "output: when target node has a local alarm in effect, returns a failure", context do
    failure = {:listener, :rabbit@mercurio, :lolz, 'mercurio',
                  {0, 0, 0, 0, 0, 0, 0, 0}, 7761613,
                  [backlog: 128, nodelay: true]}
    assert match?({:error, _}, @command.output({false, [failure]}, context[:opts]))
  end
end
