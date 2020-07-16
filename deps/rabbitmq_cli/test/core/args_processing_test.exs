## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule ArgsProcessingTest do
  use ExUnit.Case, async: false
  import TestHelper

  defp list_commands() do
    [
      RabbitMQ.CLI.Ctl.Commands.ListBindingsCommand,
      RabbitMQ.CLI.Ctl.Commands.ListChannelsCommand,
      RabbitMQ.CLI.Ctl.Commands.ListConnectionsCommand,
      RabbitMQ.CLI.Ctl.Commands.ListConsumersCommand,
      RabbitMQ.CLI.Ctl.Commands.ListExchangesCommand,
      RabbitMQ.CLI.Ctl.Commands.ListQueuesCommand,
      RabbitMQ.CLI.Ctl.Commands.ListVhostsCommand
    ]
  end

  defp all_commands() do
    RabbitMQ.CLI.Core.CommandModules.load_commands(:all, %{})
    |> Map.values
  end

  defp line_filter([_, description]) do
    Regex.match?(~r/must be one of/, description)
  end
  defp line_filter(line) do
    Regex.match?(~r/must be one of/, line)
  end

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    :ok
  end

  setup context do
    on_exit(context, fn -> delete_user(context[:user]) end)
    {:ok, opts: %{node: get_rabbit_hostname(), timeout: 50_000, vhost: "/"}}
  end

  test "defaults are merged with positinal args", _context do
    commands = all_commands()
    Enum.each(commands,
      fn(command) ->
        command.merge_defaults([], %{})
        command.merge_defaults(["arg"], %{})
        command.merge_defaults(["two", "args"], %{})
        command.merge_defaults(["even", "more", "args"], %{})

        command.merge_defaults([], %{unknown: "option"})
        command.merge_defaults(["arg"], %{unknown: "option"})
      end)
  end

  # this test parses info keys mentioned in the usage_additional section
  # and makes sure they pass validation, including when separated by a comma
  # or a mix of commas and spaces
  test "comma-separated info items are supported", context do
    commands = list_commands()
    Enum.each(commands, fn(command) ->
      items_usage = case command.usage_additional() do
        # find the line with info items, ignore the rest
        list when is_list(list) ->
          # list items can be strings or pairs
          Enum.filter(list, &line_filter/1) |> List.first |> Enum.join(" ")
        string ->
          string
      end
      # info_item, info_item2, …
      case Regex.run(~r/.*one of (.*)$/, items_usage, [capture: :all_but_first]) do
        nil          ->
          throw "Command #{command} does not list info items in usage_additional or the format has changed. Output: #{items_usage}"
        [info_items] ->
          :ok = command.validate([info_items], context[:opts])
          :ok = command.validate(String.split(info_items, " "), context[:opts])
          run_command_ok(command, [info_items], context[:opts])
          run_command_ok(command, String.split(info_items, " "), context[:opts])
      end
    end)
  end

  def run_command_ok(command, args_init, options_init) do
    {args, options} = command.merge_defaults(args_init, options_init)
    assert_stream_without_errors(command.run(args, options))
  end
end
