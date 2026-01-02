## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule EnableVhostDeletionProtectionCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.EnableVhostDeletionProtectionCommand
  @inverse_command RabbitMQ.CLI.Ctl.Commands.DisableVhostDeletionProtectionCommand
  @vhost "enable-vhost-deletion-protection"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  setup context do
    on_exit(context, fn -> delete_vhost(context[:vhost]) end)
    :ok
  end

  test "validate: no arguments fails validation" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: too many arguments fails validation" do
    assert @command.validate(["test", "extra"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: virtual host name without options fails validation" do
    assert @command.validate(["a-vhost"], %{}) == :ok
  end

  test "run: enabling deletion protection succeeds", context do
    add_vhost(@vhost)

    assert @command.run([@vhost], context[:opts]) == :ok
    vh = find_vhost(@vhost)
    assert vh[:protected_from_deletion]

    assert @inverse_command.run([@vhost], context[:opts]) == :ok
    vh = find_vhost(@vhost)
    assert !vh[:protected_from_deletion]

    delete_vhost(@vhost)
  end

  test "run: attempt to use a non-existent virtual host fails", context do
    vh = "a-non-existent-3882-vhost"

    assert match?(
             {:error, {:no_such_vhost, _}},
             @command.run([vh], Map.merge(context[:opts], %{}))
           )
  end

  test "run: attempt to use an unreachable node returns a nodedown" do
    opts = %{node: :jake@thedog, timeout: 200, description: "does not matter"}
    assert match?({:badrpc, _}, @command.run(["na"], opts))
  end

  test "banner", context do
    assert @command.banner([@vhost], context[:opts]) =~
             ~r/Protecting virtual host/
  end
end
