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
        openssl_format: true,
        erlang_format: false,
        map_format: false,
        all: false
      }}
  end

  test "validate: providing no arguments passes validation", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "validate: providing --openssl-format passes validation", context do
    assert @command.validate([], Map.merge(context[:opts], %{openssl_format: true})) == :ok
  end

  test "validate: providing --erlang-format passes validation", context do
    assert @command.validate([], Map.merge(context[:opts], %{erlang_format: true, openssl_format: false})) == :ok
  end

  test "validate: providing any arguments fails validation", context do
    assert @command.validate(["a", "b", "c"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  test "validate: setting all formats to false fails validation", context do
    assert @command.validate([], Map.merge(context[:opts],
                                           %{openssl_format: false,
                                             erlang_format: false,
                                             map_format: false})) ==
      {:validation_failure, {:bad_argument, "At least one format must be selected"}}
  end

  test "validate: setting both --openssl-format and --erlang-format to true fails validation", context do
    assert @command.validate([], Map.merge(context[:opts], %{openssl_format: true, erlang_format: true})) ==
      {:validation_failure, {:bad_argument, "Cannot use multiple formats together"}}
  end

  test "validate: treats empty positional arguments and default switches as a success" do
    assert @command.validate([], %{openssl_format: true,
                                   erlang_format: false}) == :ok
  end

  test "validate: treats empty positional arguments Erlang term format flag and default flag as a success" do
    assert @command.validate([], %{erlang_format: true}) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?({:badrpc, _}, @command.run([], Map.merge(context[:opts], %{node: :jake@thedog})))
  end

  test "run: returns a list of cipher suites in OpenSSL format", context do
    res = @command.run([], context[:opts])
    for cipher <- res, do: assert true == is_list(cipher)
    # the list is long and its values are environment-specific,
    # so we simply assert that it is non-empty. MK.
    assert length(res) > 0
  end

  test "run: returns a list of cipher suites in erlang format", context do
    res = @command.run([], Map.merge(context[:opts], %{openssl_format: false,
                                                       erlang_format: true}))

    for cipher <- res, do: assert true = is_tuple(cipher)
    # the list is long and its values are environment-specific,
    # so we simply assert that it is non-empty. MK.
    assert length(res) > 0
  end

  test "run: returns a list of cipher suites in map format", context do
    res = @command.run([], Map.merge(context[:opts], %{openssl_format: false,
                                                       map_format: true}))
    for cipher <- res, do: assert true = is_map(cipher)
    # the list is long and its values are environment-specific,
    # so we simply assert that it is non-empty. MK.
    assert length(res) > 0
  end

  test "run: returns more cipher suites when all suites requested", context do
    all_suites_opts = Map.merge(context[:opts], %{all: true})
    default_suites_opts = Map.merge(context[:opts], %{all: false})
    all_suites = @command.run([], all_suites_opts)
    default_suites = @command.run([], default_suites_opts)
    assert length(all_suites) > length(default_suites)
    assert length(default_suites -- all_suites) == 0
  end

end
