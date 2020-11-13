## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule ResolveHostnameCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.ResolveHostnameCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    start_rabbitmq_app()

    ExUnit.configure([max_cases: 1])
    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 30000,
        address_family: "ipv4",
        offline: false
      }}
  end

  test "merge_defaults: defaults to IPv4 address family" do
    assert @command.merge_defaults([], %{}) == {[], %{address_family: "IPv4", offline: false}}
  end

  test "validate: a single positional argument passes validation" do
    assert @command.validate(["rabbitmq.com"], %{}) == :ok
  end

  test "validate: treats positional arguments as a failure" do
    assert @command.validate(["elixir-lang.org", "extra-arg"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: address family other than IPv4 or IPv6 fails validation" do
    assert match?({:validation_failure, {:bad_argument, _}},
                  @command.validate(["elixir-lang.org"], %{address_family: "ipv5"}))

    assert match?({:validation_failure, {:bad_argument, _}},
                  @command.validate(["elixir-lang.org"], %{address_family: "IPv5"}))
  end

  test "validate: IPv4 for address family passes validation" do
    assert @command.validate(["elixir-lang.org"], %{address_family: "ipv4"}) == :ok
    assert @command.validate(["elixir-lang.org"], %{address_family: "IPv4"}) == :ok
  end

  test "validate: IPv6 for address family passes validation" do
    assert @command.validate(["elixir-lang.org"], %{address_family: "ipv6"}) == :ok
    assert @command.validate(["elixir-lang.org"], %{address_family: "IPv6"}) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    opts = Map.merge(context[:opts], %{node: :jake@thedog, timeout: 100})
    assert match?({:badrpc, _}, @command.run(["elixir-lang.org"], opts))
  end

  test "run: returns a resolution result", context do
    case @command.run(["github.com"], context[:opts]) do
      {:ok, _hostent}     -> :ok
      {:error, :nxdomain} -> :ok
      other -> flunk("hostname resolution returned #{other}")
    end
  end

  test "run with --offline: returns a resolution result", context do
    case @command.run(["github.com"], Map.merge(context[:opts], %{offline: true})) do
      {:ok, _hostent}     -> :ok
      {:error, :nxdomain} -> :ok
      other -> flunk("hostname resolution returned #{other}")
    end
  end
end
