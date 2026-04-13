## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule ListChannelInterceptorsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListChannelInterceptorsCommand

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

  test "merge_defaults: adds table_headers true to opts", context do
    {args, opts} = @command.merge_defaults([], context[:opts])
    assert args == []
    assert opts[:table_headers] == true
  end

  test "validate: accepts no positional arguments", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "validate: rejects any positional arguments", context do
    assert @command.validate(["extra"], context[:opts]) ==
             {:validation_failure, :too_many_args}
  end

  test "run: on a bad RabbitMQ node, returns a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run([], opts))
  end

  @tag test_timeout: :infinity
  test "run: with no interceptors registered, returns an empty list", context do
    result = @command.run([], context[:opts])
    assert result == []
  end

  @tag test_timeout: :infinity
  test "run: registered interceptors are listed with applies_to joined as a string", context do
    node = get_rabbit_hostname()

    :ok =
      :rabbit_misc.rpc_call(
        node,
        :rabbit_registry,
        :register,
        [:channel_interceptor, <<"test interceptor">>, :dummy_interceptor]
      )

    try do
      result = @command.run([], context[:opts])
      assert is_list(result)
      interceptor = Enum.find(result, fn info -> info[:name] == :dummy_interceptor end)
      assert interceptor != nil
      assert is_binary(interceptor[:applies_to])
      assert is_integer(interceptor[:priority])
    after
      :rabbit_misc.rpc_call(
        node,
        :rabbit_registry,
        :unregister,
        [:channel_interceptor, <<"test interceptor">>]
      )
    end
  end

  test "banner: returns the expected string", context do
    assert @command.banner([], context[:opts]) == "Listing channel interceptors ..."
  end
end
