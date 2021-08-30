## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


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
