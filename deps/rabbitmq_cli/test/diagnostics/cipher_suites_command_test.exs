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


defmodule CipherSuitesCommandTest do
  use ExUnit.Case
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.CipherSuitesCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()


    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 30000,
        openssl_format: context[:openssl_format] || false
      }}
  end

  test "merge_defaults: defaults to the Erlang term format (that is, non-OpenSSL)" do
    assert @command.merge_defaults([], %{}) == {[], %{openssl_format: false}}
  end

  test "merge_defaults: OpenSSL format can be switched on" do
    assert @command.merge_defaults([], %{openssl_format: true}) == {[], %{openssl_format: true}}
  end

  test "merge_defaults: OpenSSL format can be switched off" do
    assert @command.merge_defaults([], %{openssl_format: false}) == {[], %{openssl_format: false}}
  end

  test "validate: treats positional arguments as a failure" do
    assert @command.validate(["extra-arg"], %{openssl_format: false}) == {:validation_failure, :too_many_args}
  end

  test "validate: treats empty positional arguments and default switches as a success" do
    assert @command.validate([], %{openssl_format: false}) == :ok
  end

  test "validate: treats empty positional arguments and an OpenSSL format flag as a success" do
    assert @command.validate([], %{openssl_format: true}) == :ok
  end

  @tag test_timeout: 0
  test "run: targeting an unreachable node throws a badrpc", context do
    target = :jake@thedog

    opts = %{node: target}
    assert @command.run([], Map.merge(context[:opts], opts)) == {:badrpc, :nodedown}
  end

  test "run: returns a list of cipher suites", context do
    res = @command.run([], context[:opts])
    # the list is long and its values are environment-specific,
    # so we simply assert that it is non-empty. MK.
    assert length(res) > 0
  end

  @tag openssl_format: true
  test "run: returns a list cipher suites in the OpenSSL format", context do
    res = @command.run([], context[:opts])
    # see the test above
    assert length(res) > 0
  end
end
