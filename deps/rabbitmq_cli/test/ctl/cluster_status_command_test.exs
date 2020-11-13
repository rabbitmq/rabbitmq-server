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


defmodule ClusterStatusCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ClusterStatusCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()


    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "validate: argument count validates", context do
    assert @command.validate([], context[:opts]) == :ok
    assert @command.validate(["extra"], context[:opts]) ==
    {:validation_failure, :too_many_args}
  end

  test "run: status request on a named, active RMQ node is successful", context do
    assert @command.run([], context[:opts])[:nodes] != nil
  end

  test "run: status request on nonexistent RabbitMQ node returns a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "banner", context do
    s = @command.banner([], context[:opts])

    assert s =~ ~r/Cluster status of node/
    assert s =~ ~r/#{get_rabbit_hostname()}/
  end
end
