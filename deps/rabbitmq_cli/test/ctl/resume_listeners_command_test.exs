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
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule ResumeListenersCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ResumeListenersCommand

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

  test "run: resumes all client TCP listeners so new client connects are accepted", context do
    suspend_all_client_listeners()
    expect_client_connection_failure()

    assert @command.run([], Map.merge(context[:opts], %{timeout: 5_000})) == :ok

    # implies a successful connection
    with_channel("/", fn _ -> :ok end)
    close_all_connections(get_rabbit_hostname())
  end
end
