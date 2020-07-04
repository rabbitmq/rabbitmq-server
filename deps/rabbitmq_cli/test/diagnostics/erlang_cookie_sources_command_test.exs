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

defmodule ErlangCookieSourcesCommandTest do
  use ExUnit.Case, async: true

  @command RabbitMQ.CLI.Diagnostics.Commands.ErlangCookieSourcesCommand

  setup context do
    {:ok, opts: %{}}
  end

  test "merge_defaults: merges no defaults" do
    assert @command.merge_defaults([], %{}) == {[], %{}}
  end

  test "validate: treats positional arguments as a failure" do
    assert @command.validate(["extra-arg"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: treats empty positional arguments and default switches as a success" do
    assert @command.validate([], %{}) == :ok
  end

  test "run: returns Erlang cookie sources info", context do
    result = @command.run([], context[:opts])

    assert result[:effective_user] != nil
    assert result[:home_dir] != nil
    assert result[:cookie_file_path] != nil
    assert result[:cookie_file_exists] != nil
    assert result[:cookie_file_access] != nil
  end
end
