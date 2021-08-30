## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule SetVhostTagsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetVhostTagsCommand

  @vhost    "vhost99-tests"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_vhost(@vhost)

    on_exit([], fn ->
      delete_vhost(@vhost)
    end)

    :ok
  end

  setup context do
    add_vhost(context[:vhost])
    on_exit([], fn -> delete_vhost(context[:vhost]) end)

    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "validate: on an incorrect number of arguments, returns an error" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "run: throws a badrpc when instructed to contact an unreachable RabbitMQ node" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run([@vhost, :qa], opts))
  end

  @tag vhost: @vhost
  test "run: with a single optional argument, adds a single tag", context  do
    @command.run([context[:vhost], :qa], context[:opts])

    result = Enum.find(
      list_vhosts(),
      fn(record) -> record[:vhost] == context[:vhost] end
    )

    assert result[:tags] == context[:tags]
  end

  @tag vhost: "non/ex1st3nT"
  test "run: when virtual host does not exist, reports an error", context do
    delete_vhost(context[:vhost])

    assert @command.run(
      [context[:vhost]],
      context[:opts]
    ) == {:error, {:no_such_vhost, context[:vhost]}}
  end

  @tag user: @vhost, tags: [:qa, :limited]
  test "run: with multiple optional arguments, adds multiple tags", context  do
    @command.run(
      [context[:vhost] | context[:tags]],
      context[:opts]
    )

    result = Enum.find(
      list_vhosts(),
      fn(record) -> record[:vhost] == context[:vhost] end
    )

    assert result[:tags] == context[:tags]
  end

  @tag user: @vhost, tags: [:qa]
  test "run: with no optional arguments, clears virtual host tags", context  do
    set_vhost_tags(context[:vhost], context[:tags])

    @command.run([context[:vhost]], context[:opts])

    result = Enum.find(
      list_vhosts(),
      fn(record) -> record[:vhost] == context[:vhost] end
    )

    assert result[:tags] == []
  end

  @tag user: @vhost, tags: [:qa]
  test "run: identical calls are idempotent", context  do
    set_vhost_tags(context[:vhost], context[:tags])

    assert @command.run(
      [context[:vhost] | context[:tags]],
      context[:opts]
    ) == :ok

    result = Enum.find(
      list_vhosts(),
      fn(record) -> record[:vhost] == context[:vhost] end
    )

    assert result[:tags] == context[:tags]
  end

  @tag user: @vhost, old_tags: [:qa], new_tags: [:limited]
  test "run: overwrites existing tags them", context  do
    set_vhost_tags(context[:vhost], context[:old_tags])

    assert @command.run(
      [context[:vhost] | context[:new_tags]],
      context[:opts]
    ) == :ok

    result = Enum.find(
      list_vhosts(),
      fn(record) -> record[:vhost] == context[:vhost] end
    )

    assert result[:tags] == context[:new_tags]
  end

  @tag user: @vhost, tags: ["abc"]
  test "banner", context  do
    assert @command.banner(
        [context[:vhost] | context[:tags]],
        context[:opts]
      )
      =~ ~r/Setting tags for virtual host \"#{context[:vhost]}\" to \[#{context[:tags]}\] \.\.\./
  end

end
