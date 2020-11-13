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


defmodule TraceOffCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.TraceOffCommand

  @test_vhost "test"
  @default_vhost "/"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_vhost(@test_vhost)

    on_exit([], fn ->
      delete_vhost(@test_vhost)


    end)

    :ok
  end

  setup context do
    trace_on(context[:vhost])
    on_exit(context, fn -> trace_off(context[:vhost]) end)
    {:ok, opts: %{node: get_rabbit_hostname(), vhost: context[:vhost]}}
  end

  test "merge_defaults: defaults can be overridden" do
    assert @command.merge_defaults([], %{}) == {[], %{vhost: "/"}}
    assert @command.merge_defaults([], %{vhost: "non_default"}) == {[], %{vhost: "non_default"}}
  end

  test "validate: wrong number of arguments triggers arg count error" do
    assert @command.validate(["extra"], %{}) == {:validation_failure, :too_many_args}
  end

  test "run: on an active node, trace_off command works on default" do
    opts = %{node: get_rabbit_hostname()}
    opts_with_vhost = %{node: get_rabbit_hostname(), vhost: "/"}
    trace_on(@default_vhost)

    assert @command.merge_defaults([], opts) == {[], opts_with_vhost}
  end

  test "run: on an invalid RabbitMQ node, return a nodedown" do
    opts = %{node: :jake@thedog, vhost: "/", timeout: 200}
    assert match?({:badrpc, _}, @command.run([], opts))
  end

  @tag target: get_rabbit_hostname(), vhost: @default_vhost
  test "run: calls to trace_off are idempotent", context do
    @command.run([], context[:opts])
    assert @command.run([], context[:opts]) == {:ok, "Trace disabled for vhost #{@default_vhost}"}
  end

  @tag vhost: @test_vhost
  test "run: on an active node, trace_off command works on named vhost", context do
    assert @command.run([], context[:opts]) == {:ok, "Trace disabled for vhost #{@test_vhost}"}
  end

  @tag vhost: "toast"
  test "run: Turning tracing off on invalid host returns successfully", context do
    assert @command.run([], context[:opts]) == {:ok, "Trace disabled for vhost toast"}
  end

  @tag vhost: @default_vhost
  test "banner", context do
    assert @command.banner([], context[:opts])
      =~ ~r/Stopping tracing for vhost "#{context[:vhost]}" .../
  end
end
