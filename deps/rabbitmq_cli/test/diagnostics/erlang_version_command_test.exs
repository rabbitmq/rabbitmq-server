## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ErlangVersionCommandTest do
  use ExUnit.Case
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.ErlangVersionCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 30000,
        details: false,
        offline: false
      }}
  end

  test "merge_defaults: defaults to remote version and abbreviated output" do
    assert @command.merge_defaults([], %{}) == {[], %{details: false, offline: false}}
  end

  test "validate: treats positional arguments as a failure" do
    assert @command.validate(["extra-arg"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: treats empty positional arguments and default switches as a success" do
    assert @command.validate([], %{}) == :ok
  end

  test "validate: treats empty positional arguments and --details as a success" do
    assert @command.validate([], %{details: true}) == :ok
  end

  test "validate: treats empty positional arguments and --offline as a success" do
    assert @command.validate([], %{offline: true}) == :ok
  end

  test "validate: treats empty positional arguments, --details and --offline as a success" do
    assert @command.validate([], %{details: true, offline: true}) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?({:badrpc, _}, @command.run([], Map.merge(context[:opts], %{node: :jake@thedog, details: false})))
  end

  test "run: returns Erlang/OTP version on the target node", context do
    res = @command.run([], context[:opts])
    assert is_bitstring(res)
  end

  test "run with --details: returns Erlang/OTP version on the target node", context do
    res = @command.run([], Map.merge(%{details: true}, context[:opts]))
    assert is_bitstring(res)
  end

  test "run: when --offline is used, returns local Erlang/OTP version", context do
    res = @command.run([], Map.merge(context[:opts], %{offline: true}))
    assert is_bitstring(res)
  end
end
