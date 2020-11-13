## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule PurgeQueueCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.PurgeQueueCommand
  @user "guest"
  @vhost "purge-queue-vhost"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        vhost: @vhost,
        timeout: context[:test_timeout]
      }}
  end

  test "merge_defaults: defaults can be overridden" do
    assert @command.merge_defaults([], %{}) == {[], %{vhost: "/"}}
    assert @command.merge_defaults([], %{vhost: "non_default"}) == {[], %{vhost: "non_default"}}
  end

  @tag test_timeout: 30000
  test "request to an existent queue on active node succeeds", context do
    add_vhost @vhost
    set_permissions @user, @vhost, [".*", ".*", ".*"]
    on_exit(context, fn -> delete_vhost(@vhost) end)

    q = "foo"
    n = 20

    declare_queue(q, @vhost)
    assert message_count(@vhost, q) == 0

    publish_messages(@vhost, q, n)
    assert message_count(@vhost, q) == n

    assert @command.run([q], context[:opts]) == :ok
    assert message_count(@vhost, q) == 0
  end

  @tag test_timeout: 30000
  test "request to a non-existent queue on active node returns not found", context do
    assert @command.run(["non-existent"], context[:opts]) == {:error, :not_found}
  end

  @tag test_timeout: 0
  test "run: timeout causes command to return a bad RPC", context do
    assert @command.run(["foo"], context[:opts]) == {:badrpc, :timeout}
  end

  test "shows up in help" do
    s = @command.usage()
    assert s =~ ~r/purge_queue/
  end

  test "defaults to vhost /" do
    assert @command.merge_defaults(["foo"], %{bar: "baz"}) == {["foo"], %{bar: "baz", vhost: "/"}}
  end

  test "validate: with extra arguments returns an arg count error" do
    assert @command.validate(["queue-name", "extra"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: with no arguments returns an arg count error" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: with correct args returns ok" do
    assert @command.validate(["q"], %{}) == :ok
  end

  test "banner informs that vhost's queue is purged" do
    assert @command.banner(["my-q"], %{vhost: "/foo"}) == "Purging queue 'my-q' in vhost '/foo' ..."
  end
end
