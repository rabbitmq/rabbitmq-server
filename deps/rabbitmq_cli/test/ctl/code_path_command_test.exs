## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule CodePathCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.CodePathCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    paths = get_paths(get_rabbit_hostname())
    on_exit(
      fn ->
        :rabbit_misc.rpc_call(
          node,
          :code,
          :del_path,
          ['/tmp']
        )
      end)
    :ok
  end

  setup context do
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  def get_paths(node) do
    :rabbit_misc.rpc_call(
      node,
      :code,
      :get_path,
      []
    )
  end

  def get_paths_reversed(node) do
    Enum.reverse(get_paths(node))
  end

  test "run: add then delete tmp folder", context do
    assert @command.run(["add", "/tmp"], context[:opts]) == :ok
    paths = get_paths_reversed(context[:opts][:node])
    assert hd(paths) == '/tmp'

    assert @command.run(["delete", "/tmp"], context[:opts]) == :ok
    paths = get_paths_reversed(context[:opts][:node])
    assert hd(paths) != '/tmp'
  end

  test "run: delete non-existing folder", context do
    assert @command.run(["delete", "/this/tmp/folder/please/dont/exist"], context[:opts]) == {:false, "/this/tmp/folder/please/dont/exist"}
  end

  test "validate: not enough arguments", context do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["foo"], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: invalid selector", context do
    assert @command.validate(["replace", "/tmp"], %{}) == {:validation_failure, {:bad_argument, "Selector needs to be either 'add' or 'delete'."}}
  end

  test "validate: delete too many arg", context do
    assert @command.validate(["delete", "/tmp", "/foo"], %{}) == {:validation_failure, {:bad_argument, "'delete' selector only takes one directory argument"}}
  end
end
