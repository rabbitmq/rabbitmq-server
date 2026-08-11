## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule SetChannelInterceptorPrioritiesCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetChannelInterceptorPrioritiesCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {
      :ok,
      opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || :infinity
      }
    }
  end

  test "merge_defaults: leaves args and opts unchanged", context do
    assert @command.merge_defaults(["dummy_interceptor", "1"], context[:opts]) ==
             {["dummy_interceptor", "1"], context[:opts]}
  end

  test "validate: rejects no arguments", context do
    assert @command.validate([], context[:opts]) ==
             {:validation_failure, :not_enough_args}
  end

  test "validate: rejects an odd number of arguments", context do
    assert match?(
             {:error, _},
             @command.validate(["dummy_interceptor"], context[:opts])
           )
  end

  test "validate: rejects a non-integer priority", context do
    assert match?(
             {:error, _},
             @command.validate(["dummy_interceptor", "high"], context[:opts])
           )
  end

  test "validate: accepts interceptor and priority pairs", context do
    assert @command.validate(["dummy_interceptor", "1"], context[:opts]) == :ok

    assert @command.validate(
             ["dummy_interceptor_a", "1", "dummy_interceptor_b", "-2"],
             context[:opts]
           ) == :ok
  end

  test "run: on a bad RabbitMQ node, returns a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run(["dummy_interceptor", "1"], opts))
  end

  @tag test_timeout: :infinity
  test "run: sets the priority of a registered interceptor", context do
    node = get_rabbit_hostname()

    :ok =
      :rabbit_misc.rpc_call(
        node,
        :rabbit_registry,
        :register,
        [:channel_interceptor, <<"test interceptor">>, :dummy_interceptor]
      )

    try do
      assert @command.run(["dummy_interceptor", "7"], context[:opts]) == :ok

      priorities =
        :rabbit_misc.rpc_call(
          node,
          :application,
          :get_env,
          [:rabbit, :channel_interceptor_priorities, []]
        )

      assert {:dummy_interceptor, 7} in priorities
    after
      :rabbit_misc.rpc_call(
        node,
        :application,
        :unset_env,
        [:rabbit, :channel_interceptor_priorities]
      )

      :rabbit_misc.rpc_call(
        node,
        :rabbit_registry,
        :unregister,
        [:channel_interceptor, <<"test interceptor">>]
      )
    end
  end

  test "banner: describes the priorities being set", context do
    assert @command.banner(
             ["dummy_interceptor_a", "1", "dummy_interceptor_b", "2"],
             context[:opts]
           ) ==
             "Setting channel interceptor priorities to [dummy_interceptor_a=1, dummy_interceptor_b=2] ..."
  end
end
