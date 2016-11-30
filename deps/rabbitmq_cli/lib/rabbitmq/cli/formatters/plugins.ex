## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Formatters.Plugins do
  @behaviour RabbitMQ.CLI.FormatterBehaviour

  def format_error(err, _) when is_binary(err) do
    err
  end

  def format_output(%{status: status, format: format, plugins: plugins},
                    options) do
    legend(status, format, options) ++ format_plugins(plugins, format)
  end
  def format_output(%{enabled: enabled, mode: _} = output, options) do
    case length(enabled) do
      0 -> ["Plugin configuration unchanged."];
      _ -> ["The following plugins have been enabled:" |
            for plugin <- enabled do "  #{plugin}" end] ++
            applying(output, options)
    end
  end
  def format_output(%{disabled: disabled, mode: _} = output, options) do
    case length(disabled) do
      0 -> ["Plugin configuration unchanged."];
      _ -> ["The following plugins have been disabled:" |
            for plugin <- disabled do "  #{plugin}" end] ++
            applying(output, options)
    end
  end
  def format_output(%{set: set, mode: _} = output, options) do
    case length(set) do
      0 -> ["Plugin configuration unchanged."];
      _ -> ["The following plugins have been enabled:" |
            for plugin <- set do "  #{plugin}" end] ++
            applying(output, options)
    end
  end

  def format_stream(stream, options) do
    ## PLugins commands never return stream
    format_output(stream, options)
  end

  defp format_plugins(plugins, format) do
    max_name_length = Enum.reduce(plugins, 0,
                                  fn(%{name: name}, len) ->
                                    max(String.length(to_string(name)), len)
                                  end)
    for plugin <- plugins do
      format_plugin(plugin, format, max_name_length)
    end
    |> List.flatten
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
    enabled_sign = case enabled do
      :implicit -> "e";
      :enabled  -> "E";
      :not_enabled -> " "
    end
    running_sign = case running do
      true  -> "*";
      false -> " "
    end

    "[#{enabled_sign}#{running_sign}] #{name}"
  end

  defp inline_version(%{name: name} = plugin, max_name_length) do
    spacing = String.duplicate(" ", max_name_length -
                                    String.length(to_string(name)))
    spacing <> " " <> augment_version(plugin)
  end

  defp verbose(%{dependencies: dependencies,
                 description: description} = plugin) do
    prettified = to_string(:io_lib.format("~p", [dependencies]))
    [
      "     Version:     \t#{augment_version(plugin)}",
      "     Dependencies:\t#{prettified}",
      "     Description: \t#{description}"
    ]

  end

  defp augment_version(%{version: version, running_version: nil} ) do
    to_string(version)
  end

  defp augment_version(%{version: version, running_version: version}) do
    to_string(version)
  end

  defp augment_version(%{version: version, running_version: running_version}) do
    "#{running_version} (pending upgrade to #{version})"
  end

  ## Do not print legend in minimal mode
  defp legend(_, :minimal, _) do
    []
  end
  defp legend(status, _, %{node: node}) do
    [" Configured: E = explicitly enabled; e = implicitly enabled",
     " | Status: #{status_message(status, node)}",
     " |/"]
  end

  defp status_message(:running, node) do
    "* = running on #{node}"
  end
  defp status_message(:node_down, node) do
    "[failed to contact #{node} - status not shown]"
  end

  defp applying(%{mode: :offline}, _) do
    []
  end
  defp applying(%{mode: :online, started: started, stopped: stopped}, %{node: node}) do
    stopped_message = case length(stopped) do
      0   -> [];
      len -> ["stopped #{len} plugins"]
    end
    started_message = case length(started) do
      0   -> [];
      len -> ["started #{len} plugins"]
    end
    change_message = case Enum.join(started_message ++ stopped_message, " and ") do
      ""  -> "nothing to do";
      msg -> msg
    end
    ["",
     "Applying plugin configuration to #{node}... " <> change_message <> "."]
  end

end
