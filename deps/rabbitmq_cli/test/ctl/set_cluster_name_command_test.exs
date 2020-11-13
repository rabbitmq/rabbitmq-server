## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


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
