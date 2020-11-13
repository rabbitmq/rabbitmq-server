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

defmodule TlsVersionsCommandTest do
  use ExUnit.Case
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.TlsVersionsCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 30000
      }}
  end

  test "merge_defaults: is a no-op" do
    assert @command.merge_defaults([], %{}) == {[], %{}}
  end

  test "validate: treats positional arguments as a failure" do
    assert @command.validate(["extra-arg"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: treats empty positional arguments and default switches as a success" do
    assert @command.validate([], %{}) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?({:badrpc, _}, @command.run([], Map.merge(context[:opts], %{node: :jake@thedog})))
  end

  test "run when formatter is set to JSON: return a document with a list of supported TLS versions", context do
    m  = @command.run([], Map.merge(context[:opts], %{formatter: "json"})) |> Map.new
    xs = Map.get(m, :available)

    # assert that we have a list and tlsv1.2 is included
    assert length(xs) > 0
    assert Enum.member?(xs, :"tlsv1.2")
  end

  test "run and output: return a list of supported TLS versions", context do
    m          = @command.run([], context[:opts])
    {:ok, res} = @command.output(m, context[:opts])

    # assert that we have a list and tlsv1.2 is included
    assert length(res) > 0
    assert Enum.member?(res, :"tlsv1.2")
  end
end
