## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


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
        format: context[:format] || "openssl",
        all: false
      }}
  end

  test "merge_defaults: defaults to the OpenSSL format" do
    assert @command.merge_defaults([], %{}) == {[], %{format: "openssl", all: false}}
  end

  test "merge_defaults: format is case insensitive" do
    assert @command.merge_defaults([], %{format: "OpenSSL"}) == {[], %{format: "openssl", all: false}}
    assert @command.merge_defaults([], %{format: "Erlang"}) == {[], %{format: "erlang", all: false}}
    assert @command.merge_defaults([], %{format: "Map"}) == {[], %{format: "map", all: false}}
  end

  test "merge_defaults: format can be overridden" do
    assert @command.merge_defaults([], %{format: "map"}) == {[], %{format: "map", all: false}}
  end

  test "validate: treats positional arguments as a failure", context do
    assert @command.validate(["extra-arg"], context[:opts]) == {:validation_failure, :too_many_args}
  end

  test "validate: treats empty positional arguments and default switches as a success", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "validate: supports openssl, erlang and map formats", context do
    assert @command.validate([], Map.merge(context[:opts], %{format: "openssl"})) == :ok
    assert @command.validate([], Map.merge(context[:opts], %{format: "erlang"})) == :ok
    assert @command.validate([], Map.merge(context[:opts], %{format: "map"})) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?({:badrpc, _}, @command.run([], Map.merge(context[:opts], %{node: :jake@thedog})))
  end

  @tag format: "openssl"
  test "run: returns a list of cipher suites in OpenSSL format", context do
    res = @command.run([], context[:opts])
    for cipher <- res, do: assert true == is_list(cipher)
    # the list is long and its values are environment-specific,
    # so we simply assert that it is non-empty. MK.
    assert length(res) > 0
  end

  @tag format: "erlang"
  test "run: returns a list of cipher suites in erlang format", context do
    res = @command.run([], context[:opts])

    for cipher <- res, do: assert true = is_tuple(cipher)
    # the list is long and its values are environment-specific,
    # so we simply assert that it is non-empty. MK.
    assert length(res) > 0
  end

  @tag format: "map"
  test "run: returns a list of cipher suites in map format", context do
    res = @command.run([], context[:opts])
    for cipher <- res, do: assert true = is_map(cipher)
    # the list is long and its values are environment-specific,
    # so we simply assert that it is non-empty. MK.
    assert length(res) > 0
  end

  test "run: returns more cipher suites when all suites requested", context do
    default_suites_opts = Map.merge(context[:opts], %{all: false})
    default_suites = @command.run([], default_suites_opts)

    all_suites_opts = Map.merge(context[:opts], %{all: true})
    all_suites = @command.run([], all_suites_opts)

    assert length(all_suites) > length(default_suites)
    assert length(default_suites -- all_suites) == 0
  end

end
