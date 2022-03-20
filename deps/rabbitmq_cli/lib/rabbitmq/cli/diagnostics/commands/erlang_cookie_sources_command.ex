## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.ErlangCookieSourcesCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  import RabbitMQ.CLI.Core.ANSI

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def distribution(_), do: :none

  def run([], opts) do
    switch_cookie = opts[:erlang_cookie]
    home_dir = get_home_dir()
    cookie_file_path = Path.join(home_dir, ".erlang.cookie")
    cookie_file_stat = case File.stat(Path.join(home_dir, ".erlang.cookie")) do
      {:error, :enoent} -> nil
      {:ok, value}     -> value
    end
    cookie_file_type = case cookie_file_stat do
      nil   -> nil
      value -> value.type
    end
    cookie_file_access = case cookie_file_stat do
      nil   -> nil
      value -> value.access
    end
    cookie_file_size = case cookie_file_stat do
      nil   -> nil
      value -> value.size
    end

    %{
      os_env_cookie_set: System.get_env("RABBITMQ_ERLANG_COOKIE") != nil,
      os_env_cookie_value_length: String.length(System.get_env("RABBITMQ_ERLANG_COOKIE") || ""),
      switch_cookie_set: switch_cookie != nil,
      switch_cookie_value_length: String.length(to_string(switch_cookie) || ""),
      effective_user: System.get_env("USER"),
      home_dir: home_dir,
      cookie_file_path: cookie_file_path,
      cookie_file_exists: File.exists?(cookie_file_path),
      cookie_file_type: cookie_file_type,
      cookie_file_access: cookie_file_access,
      cookie_file_size: cookie_file_size
    }
  end

  def banner([], %{}), do: "Listing Erlang cookie sources used by CLI tools..."

  def output(result, %{formatter: "json"}) do
    {:ok, result}
  end

  def output(result, _opts) do
    cookie_file_lines = [
      "#{bright("Cookie File")}\n",
      "Effective user: #{result[:effective_user] || "(none)"}",
      "Effective home directory: #{result[:home_dir] || "(none)"}",
      "Cookie file path: #{result[:cookie_file_path]}",
      "Cookie file exists? #{result[:cookie_file_exists]}",
      "Cookie file type: #{result[:cookie_file_type] || "(n/a)"}",
      "Cookie file access: #{result[:cookie_file_access] || "(n/a)"}",
      "Cookie file size: #{result[:cookie_file_size] || "(n/a)"}",
    ]

    switch_lines = [
      "\n#{bright("Cookie CLI Switch")}\n",
      "--erlang-cookie value set? #{result[:switch_cookie_set]}",
      "--erlang-cookie value length: #{result[:switch_cookie_value_length] || 0}"
    ]

    os_env_lines = [
      "\n#{bright("Env variable ")} #{bright_red("(Deprecated)")}\n",
      "RABBITMQ_ERLANG_COOKIE value set? #{result[:os_env_cookie_set]}",
      "RABBITMQ_ERLANG_COOKIE value length: #{result[:os_env_cookie_value_length] || 0}"
    ]

    lines = cookie_file_lines ++ switch_lines ++ os_env_lines

    {:ok, lines}
  end

  def help_section(), do: :configuration

  def description() do
    "Display Erlang cookie source (e.g. $HOME/.erlang.cookie file) information useful for troubleshooting"
  end

  def usage, do: "erlang_cookie_sources"

  def formatter(), do: RabbitMQ.CLI.Formatters.StringPerLine

  #
  # Implementation
  #

  @doc """
  Computes HOME directory path the same way Erlang VM/ei does,
  including taking Windows-specific env variables into account.
  """
  def get_home_dir() do
    homedrive = System.get_env("HOMEDRIVE")
    homepath  = System.get_env("HOMEPATH")

    case {homedrive != nil, homepath != nil} do
      {true, true} -> "#{homedrive}#{homepath}"
      _            -> System.get_env("HOME")
    end
  end
end
