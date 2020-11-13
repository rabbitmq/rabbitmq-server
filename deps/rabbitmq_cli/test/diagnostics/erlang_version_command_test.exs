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
