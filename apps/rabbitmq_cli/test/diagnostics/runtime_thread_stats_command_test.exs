## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule RuntimeThreadStatsCommandTest do
  use ExUnit.Case
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.RuntimeThreadStatsCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 10000,
        sample_interval: 1
      }}
  end


  test "validate: providing no arguments passes validation", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "validate: providing any arguments fails validation", context do
    assert @command.validate(["a"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  @tag test_timeout: 2000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?({:badrpc, _}, @command.run([], Map.merge(context[:opts], %{node: :jake@thedog})))
  end

  @tag test_timeout: 6000
  test "run: returns msacc-formatted output", context do
    res = @command.run([], context[:opts])
    # the output is long and its values are environment-specific,
    # so we simply assert that it is non-empty. MK.
    assert length(res) > 0
  end
end
