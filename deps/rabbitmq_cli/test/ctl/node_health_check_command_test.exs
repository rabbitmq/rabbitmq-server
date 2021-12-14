## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule NodeHealthCheckCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.NodeHealthCheckCommand

  setup do
    {:ok, opts: %{timeout: 20000}}
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate(["extra"], context[:opts]) == {:validation_failure, :too_many_args}
  end

  test "validate: with no arguments succeeds", _context do
    assert @command.validate([], []) == :ok
  end

  test "banner", context do
    assert @command.banner([], context[:opts]) |> Enum.join("\n") =~ ~r/This command has been DEPRECATED since 2019 and no longer has any effect/
  end
end
