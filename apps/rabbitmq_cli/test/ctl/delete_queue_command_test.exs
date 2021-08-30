## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule DeleteQueueCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.DeleteQueueCommand
  @user "guest"
  @vhost "delete-queue-vhost"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        vhost: @vhost,
        timeout: context[:test_timeout],
        if_empty: false,
        if_unused: false
      }}
  end

  test "merge_defaults: defaults can be overridden" do
    assert @command.merge_defaults([], %{}) == {[], %{vhost: "/", if_empty: false, if_unused: false}}
    assert @command.merge_defaults([], %{vhost: "non_default", if_empty: true}) ==
            {[], %{vhost: "non_default", if_empty: true, if_unused: false}}
  end

  test "validate: providing no queue name fails validation", context do
    assert match?(
      {:validation_failure, :not_enough_args},
      @command.validate([], context[:opts])
    )
  end

  test "validate: providing an empty queue name fails validation", context do
    assert match?(
      {:validation_failure, {:bad_argument, "queue name cannot be an empty string"}},
      @command.validate([""], context[:opts])
    )
  end

  test "validate: providing a non-blank queue name and -u succeeds", context do
    assert @command.validate(["a-queue"], %{
      node: get_rabbit_hostname(),
      vhost: @vhost,
      timeout: context[:test_timeout],
      if_unused: false
    }) == :ok
  end

  @tag test_timeout: 30000
  test "run: request to an existent queue on active node succeeds", context do
    add_vhost @vhost
    set_permissions @user, @vhost, [".*", ".*", ".*"]
    on_exit(context, fn -> delete_vhost(@vhost) end)

    q = "foo"
    n = 20

    declare_queue(q, @vhost)
    publish_messages(@vhost, q, n)

    assert @command.run([q], context[:opts]) == {:ok, n}
    {:error, :not_found} = lookup_queue(q, @vhost)
  end

  @tag test_timeout: 30000
  test "run: request to a non-existent queue on active node returns not found", context do
    assert @command.run(["non-existent"], context[:opts]) == {:error, :not_found}
  end

  @tag test_timeout: 0
  test "run: timeout causes command to return a bad RPC", context do
    add_vhost @vhost
    set_permissions @user, @vhost, [".*", ".*", ".*"]
    on_exit(context, fn -> delete_vhost(@vhost) end)

    q = "foo"
    declare_queue(q, @vhost)
    assert @command.run([q], context[:opts]) == {:badrpc, :timeout}
  end

  test "shows up in help" do
    s = @command.usage()
    assert s =~ ~r/delete_queue/
  end

  test "defaults to vhost /" do
    assert @command.merge_defaults(["foo"], %{bar: "baz"}) == {["foo"], %{bar: "baz", vhost: "/", if_unused: false, if_empty: false}}
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

  test "banner informs that vhost's queue is deleted" do
    assert @command.banner(["my-q"], %{vhost: "/foo", if_empty: false, if_unused: false}) == "Deleting queue 'my-q' on vhost '/foo' ..."
    assert @command.banner(["my-q"], %{vhost: "/foo", if_empty: true, if_unused: false}) == "Deleting queue 'my-q' on vhost '/foo' if queue is empty ..."
    assert @command.banner(["my-q"], %{vhost: "/foo", if_empty: true, if_unused: true}) == "Deleting queue 'my-q' on vhost '/foo' if queue is empty and if queue is unused ..."
  end
end
