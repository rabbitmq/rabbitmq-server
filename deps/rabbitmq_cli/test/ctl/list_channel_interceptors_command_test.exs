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
  test "run: lists interceptors registered on the node", context do
    result = @command.run([], context[:opts])
    assert is_list(result)

    interceptor =
      Enum.find(result, fn info -> info[:name] == :rabbit_sharding_interceptor end)

    assert interceptor != nil
  end

  @tag test_timeout: :infinity
  test "run: reports each interceptor's applies_to as a string and priority as an integer",
       context do
    result = @command.run([], context[:opts])

    interceptor =
      Enum.find(result, fn info -> info[:name] == :rabbit_sharding_interceptor end)

    assert interceptor != nil
    assert is_binary(interceptor[:applies_to])
    assert is_integer(interceptor[:priority])
  end

  test "banner: returns the expected string", context do
    assert @command.banner([], context[:opts]) == "Listing channel interceptors ..."
  end
end
