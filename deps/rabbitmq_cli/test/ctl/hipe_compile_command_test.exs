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
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016-2017 Pivotal Software, Inc.  All rights reserved.

defmodule HipeCompileCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  require Temp

  @command RabbitMQ.CLI.Ctl.Commands.HipeCompileCommand
  @vhost   "/"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    start_rabbitmq_app()
  end

  setup do
    rabbitmq_home = :rabbit_misc.rpc_call(node(), :code, :lib_dir, [:rabbit])

    {:ok, tmp_dir} = Temp.path

    {:ok, opts: %{
      node: get_rabbit_hostname(),
      vhost: @vhost,
      rabbitmq_home: rabbitmq_home
     },
     target_dir: tmp_dir
    }
  end

  test "validate: providing no arguments fails validation", context do
    assert @command.validate([], context[:opts]) ==
      {:validation_failure, :not_enough_args}
  end

  test "validate: providing two arguments fails validation", context do
    assert @command.validate(["/path/one", "/path/two"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  test "validate: providing three arguments fails validation", context do
    assert @command.validate(["/path/one", "/path/two", "/path/three"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  test "validate: providing one blank directory path and required options fails", context do
    assert match?({:validation_failure, {:bad_argument, _}}, @command.validate([""], context[:opts]))
  end

  test "validate: providing one path argument that only contains spaces and required options fails", context do
    assert match?({:validation_failure, {:bad_argument, _}}, @command.validate(["  "], context[:opts]))
  end

  test "validate: providing one non-blank directory path and required options succeeds", context do
    assert @command.validate([context[:target_dir]], context[:opts]) == :ok
  end

  test "validate: failure to load the rabbit application is reported as an error", context do
    assert {:validation_failure, {:unable_to_load_rabbit, _}} =
      @command.validate([context[:target_dir]], Map.delete(context[:opts], :rabbitmq_home))
  end
end
