## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule ErlangCookieSourcesCommandTest do
  use ExUnit.Case, async: true

  @command RabbitMQ.CLI.Diagnostics.Commands.ErlangCookieSourcesCommand

  setup _context do
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
