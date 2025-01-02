## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule TraceOnCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.TraceOnCommand

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
    on_exit(context, fn -> trace_off(context[:vhost]) end)
    {:ok, opts: %{node: get_rabbit_hostname(), vhost: context[:vhost]}}
  end

  test "merge_defaults: on an active node, trace_on command works on default" do
    opts = %{node: get_rabbit_hostname()}
    opts_with_vhost = %{node: get_rabbit_hostname(), vhost: "/"}

    assert @command.merge_defaults([], opts) == {[], opts_with_vhost}

    trace_off(@default_vhost)
  end

  test "merge_defaults: defaults can be overridden" do
    assert @command.merge_defaults([], %{}) == {[], %{vhost: "/"}}
    assert @command.merge_defaults([], %{vhost: "non_default"}) == {[], %{vhost: "non_default"}}
  end

  test "validate: wrong number of arguments triggers arg count error" do
    assert @command.validate(["extra"], %{}) == {:validation_failure, :too_many_args}
  end

  test "run: on an invalid RabbitMQ node, return a nodedown" do
    opts = %{node: :jake@thedog, vhost: "/", timeout: 200}

    assert match?({:badrpc, _}, @command.run([], opts))
  end

  @tag vhost: @default_vhost
  test "run: calls to trace_on are idempotent", context do
    @command.run([], context[:opts])
    assert @command.run([], context[:opts]) == {:ok, "Trace enabled for vhost #{@default_vhost}"}
  end

  @tag vhost: @test_vhost
  test "run: on an active node, trace_on command works on named vhost", context do
    assert @command.run([], context[:opts]) == {:ok, "Trace enabled for vhost #{@test_vhost}"}
  end

  @tag vhost: "toast"
  test "run: Turning tracing on on invalid host returns successfully", context do
    assert @command.run([], context[:opts]) == {:ok, "Trace enabled for vhost toast"}
  end

  @tag vhost: @default_vhost
  test "banner", context do
    assert @command.banner([], context[:opts]) =~
             ~r/Starting tracing for vhost "#{context[:vhost]}" .../
  end
end
