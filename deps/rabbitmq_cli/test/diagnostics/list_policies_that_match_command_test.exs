## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule ListPoliciesThatMatchCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.ListPoliciesThatMatchCommand

  @vhost "test1"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_vhost(@vhost)

    enable_federation_plugin()

    on_exit([], fn ->
      delete_vhost(@vhost)
    end)

    :ok
  end

  setup context do
    on_exit(context, fn ->
      clear_policy(context[:vhost], context[:key])
    end)

    {
      :ok,
      opts: %{
        node: get_rabbit_hostname(),
        vhost: context[:vhost],
        object_type: context[:object_type],
        timeout: context[:timeout] || :infinity
      }
    }
  end

  @tag vhost: @vhost
  test "merge_defaults: a well-formed command with no vhost runs against the default" do
    assert match?({_, %{vhost: "/"}}, @command.merge_defaults([], %{}))
  end

  @tag vhost: @vhost
  test "merge_defaults: does not change defined vhost" do
    assert match?(
             {[], %{vhost: "test_vhost"}},
             @command.merge_defaults([], %{vhost: "test_vhost"})
           )
  end

  @tag vhost: @vhost
  test "merge_defaults: default object_type is \"queue\"" do
    assert match?({_, %{object_type: "queue"}}, @command.merge_defaults([], %{}))

    assert match?(
             {_, %{object_type: "exchange"}},
             @command.merge_defaults([], %{object_type: "exchange"})
           )
  end

  @tag vhost: @vhost
  test "validate: providing too few arguments fails validation" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  @tag vhost: @vhost
  test "validate: providing too many arguments fails validation" do
    assert @command.validate(["too", "many"], %{}) ==
             {:validation_failure, :too_many_args}
  end

  @tag vhost: @vhost, object_type: "queue"
  test "run: it returns only the matching policies", context do
    policies = [
      %{
        vhost: @vhost,
        name: "matching-p1",
        pattern: "^foo.*",
        definition: "{\"max-length\":1}",
        apply_to: "all",
        priority: 1
      },
      %{
        vhost: @vhost,
        name: "non-matching-p10",
        pattern: "^bar.*",
        definition: "{\"max-length\":10}",
        apply_to: "all",
        priority: 10
      },
      %{
        vhost: @vhost,
        name: "matching-p0",
        pattern: "^foo.*",
        definition: "{\"max-length\":0}",
        apply_to: "all",
        priority: 0
      },
      %{
        vhost: @vhost,
        name: "matching-p2",
        pattern: "^foo.*",
        definition: "{\"max-length\":1}",
        apply_to: "all",
        priority: 2
      },
      %{
        vhost: @vhost,
        name: "non-matching-p20",
        pattern: "^foo.*",
        definition: "{\"max-length\":20}",
        apply_to: "quorum_queues",
        priority: 20
      }
    ]

    policies
    |> Enum.map(fn p ->
      set_policy(
        context[:vhost],
        p[:name],
        p[:pattern],
        p[:definition],
        p[:priority],
        p[:apply_to]
      )

      on_exit(fn ->
        clear_policy(context[:vhost], p[:name])
      end)
    end)

    declare_queue("foo", context[:vhost])
    result = for policy <- @command.run(["foo"], context[:opts]), do: Map.new(policy)

    expected = ["matching-p2", "matching-p1", "matching-p0"]
    assert Enum.map(result, fn map -> map.name end) == expected
  end

  @tag vhost: @vhost, object_type: "queue", vhost: @vhost
  test "banner_queue", context do
    opts = Map.merge(context[:opts], %{vhost: context[:vhost], object_type: "queue"})

    assert @command.banner(["my_queue"], opts) ==
             "Listing policies that match #{context[:object_type]} 'my_queue' in vhost '#{context[:vhost]}' ..."
  end

  @tag vhost: @vhost, object_type: "exchange", vhost: @vhost
  test "banner_exchange", context do
    opts = Map.merge(context[:opts], %{vhost: context[:vhost], object_type: "exchange"})

    assert @command.banner(["my_exchange"], opts) ==
             "Listing policies that match #{context[:object_type]} 'my_exchange' in vhost '#{context[:vhost]}' ..."
  end
end
