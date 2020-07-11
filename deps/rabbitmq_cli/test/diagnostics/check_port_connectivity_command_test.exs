## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule CheckPortConnectivityCommandTest do
  use ExUnit.Case, async: false
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
