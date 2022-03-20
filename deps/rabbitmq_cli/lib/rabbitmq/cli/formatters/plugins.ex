## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
alias RabbitMQ.CLI.Formatters.FormatterHelpers

defmodule RabbitMQ.CLI.Formatters.Plugins do
  @behaviour RabbitMQ.CLI.FormatterBehaviour

  def format_output(
        %{status: status, format: format, plugins: plugins},
        options
      ) do
    legend(status, format, options) ++ format_plugins(plugins, format)
  end

  def format_output(%{enabled: enabled, mode: _} = output, options) do
    case length(enabled) do
      0 ->
        ["Plugin configuration unchanged."]

      _ ->
        [
          "The following plugins have been enabled:"
          | for plugin <- enabled do
              "  #{plugin}"
            end
        ] ++
          [""] ++
          applying(output, options) ++
          log_offline(output)
    end
  end

  def format_output(%{disabled: disabled, mode: _} = output, options) do
    case length(disabled) do
      0 ->
        ["Plugin configuration unchanged."]

      _ ->
        [
          "The following plugins have been disabled:"
          | for plugin <- disabled do
              "  #{plugin}"
            end
        ] ++
          [""] ++
          applying(output, options) ++
          log_offline(output)
    end
  end

  ## Do not print enabled/disabled for set command
  def format_output(%{} = output, options) do
    applying(output, options)
  end

  def format_output([], %{node: node}) do
    ["All plugins have been disabled.", "Applying plugin configuration to #{node}..."]
  end

  def format_output(plugins, %{node: node}) when is_list(plugins) do
    [
      "The following plugins have been configured:"
      | for plugin <- plugins do
          "  #{plugin}"
        end
    ] ++
      ["Applying plugin configuration to #{node}..."]
  end

  def format_output(output, _) do
    :io_lib.format("~p", [output])
    |> to_string
  end

  def format_stream(stream, options) do
    Stream.map(
      stream,
      FormatterHelpers.without_errors_1(fn element ->
        format_output(element, options)
      end)
    )
  end

  defp format_plugins(plugins, format) do
    max_name_length =
      Enum.reduce(plugins, 0, fn %{name: name}, len ->
        max(String.length(to_string(name)), len)
      end)

    for plugin <- plugins do
      format_plugin(plugin, format, max_name_length)
    end
    |> List.flatten()
  end

  defp format_plugin(%{name: name}, :minimal, _) do
    to_string(name)
  end

  defp format_plugin(plugin, :normal, max_name_length) do
    [summary(plugin) <> inline_version(plugin, max_name_length)]
  end

  defp format_plugin(plugin, :verbose, _) do
    [summary(plugin) | verbose(plugin)]
  end

  defp summary(%{name: name, enabled: enabled, running: running}) do
    enabled_sign =
      case enabled do
        :implicit -> "e"
        :enabled -> "E"
        :not_enabled -> " "
      end

    running_sign =
      case running do
        true -> "*"
        false -> " "
      end

    "[#{enabled_sign}#{running_sign}] #{name}"
  end

  defp inline_version(%{name: name} = plugin, max_name_length) do
    spacing =
      String.duplicate(
        " ",
        max_name_length -
          String.length(to_string(name))
      )

    spacing <> " " <> augment_version(plugin)
  end

  defp verbose(%{dependencies: dependencies, description: description} = plugin) do
    prettified = to_string(:io_lib.format("~p", [dependencies]))

    [
      "     Version:     \t#{augment_version(plugin)}",
      "     Dependencies:\t#{prettified}",
      "     Description: \t#{description}"
    ]
  end

  defp augment_version(%{version: version, running_version: nil}) do
    to_string(version)
  end

  defp augment_version(%{version: version, running_version: version}) do
    to_string(version)
  end

  defp augment_version(%{version: version, running_version: running_version}) do
    "#{running_version} (pending upgrade to #{version})"
  end

  ## Do not print legend in minimal, quiet or silent mode
  defp legend(_, :minimal, _) do
    []
  end
  defp legend(_, _, %{quiet: true}) do
    []
  end
  defp legend(_, _, %{silent: true}) do
    []
  end

  defp legend(status, _, %{node: node}) do
    [
      " Configured: E = explicitly enabled; e = implicitly enabled",
      " | Status: #{status_message(status, node)}",
      " |/"
    ]
  end

  defp status_message(:running, node) do
    "* = running on #{node}"
  end

  defp status_message(:node_down, node) do
    "[failed to contact #{node} - status not shown]"
  end

  defp applying(%{mode: :offline, set: set_plugins}, _) do
    set_plugins_message =
      case length(set_plugins) do
        0 -> "nothing to do"
        len -> "set #{len} plugins"
      end

    [set_plugins_message <> "."]
  end

  defp applying(%{mode: :offline, enabled: enabled}, _) do
    enabled_message =
      case length(enabled) do
        0 -> "nothing to do"
        len -> "enabled #{len} plugins"
      end

    [enabled_message <> "."]
  end

  defp applying(%{mode: :offline, disabled: disabled}, _) do
    disabled_message =
      case length(disabled) do
        0 -> "nothing to do"
        len -> "disabled #{len} plugins"
      end

    [disabled_message <> "."]
  end

  defp applying(%{mode: :online, started: started, stopped: stopped}, _) do
    stopped_message =
      case length(stopped) do
        0 -> []
        len -> ["stopped #{len} plugins"]
      end

    started_message =
      case length(started) do
        0 -> []
        len -> ["started #{len} plugins"]
      end

    change_message =
      case Enum.join(started_message ++ stopped_message, " and ") do
        "" -> "nothing to do"
        msg -> msg
      end

    [change_message <> "."]
  end

  defp log_offline(%{mode: :offline}) do
    ["Offline change; changes will take effect at broker restart."]
  end

  defp log_offline(_) do
    []
  end
end
