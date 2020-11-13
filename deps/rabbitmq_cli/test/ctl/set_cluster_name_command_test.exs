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


defmodule SetClusterNameCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetClusterNameCommand

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])


    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "shows up in help" do
    s = @command.usage()
    assert s =~ ~r/set_cluster_name/
  end

  test "has no defaults" do
    assert @command.merge_defaults(["foo"], %{bar: "baz"}) == {["foo"], %{bar: "baz"}}
  end

  test "validate: with insufficient number of arguments, return arg count error" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: with too many arguments, return arg count error" do
    assert @command.validate(["foo", "bar"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: with correct number of arguments, return ok" do
    assert @command.validate(["mynewname"], %{}) == :ok
  end

  test "run: valid name returns ok", context do
    s = get_cluster_name()
    assert @command.run(["agoodname"], context[:opts]) == :ok
    # restore original name
    @command.run([s], context[:opts])
  end

  test "run: An invalid Rabbit node returns a bad rpc message" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run(["clustername"], opts))
  end

  test "banner shows that the name is being set" do
    s = @command.banner(["annoyyou"], %{})
    assert s == "Setting cluster name to annoyyou ..."
  end

end
