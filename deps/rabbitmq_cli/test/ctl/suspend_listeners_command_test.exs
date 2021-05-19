## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule SuspendListenersCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SuspendListenersCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    resume_all_client_listeners()

    node_name = get_rabbit_hostname()
    on_exit(fn ->
      resume_all_client_listeners()
      close_all_connections(node_name)
    end)

    {:ok, opts: %{node: node_name, timeout: 30_000}}
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "merge_defaults: merges no defaults" do
    assert @command.merge_defaults([], %{}) == {[], %{}}
  end

  test "validate: accepts no arguments", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate(["extra"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  test "run: request to a non-existent node returns a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "run: suspends all client TCP listeners so no new client connects are accepted", context do
    assert @command.run([], Map.merge(context[:opts], %{timeout: 5_000})) == :ok

    expect_client_connection_failure()
    resume_all_client_listeners()

    # implies a successful connection
    with_channel("/", fn _ -> :ok end)
    close_all_connections(get_rabbit_hostname())
  end
end
