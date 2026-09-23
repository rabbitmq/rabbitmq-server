## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Core.Input do
  alias RabbitMQ.CLI.Core.Config

  def consume_single_line_string_with_prompt(prompt, opts) do
    val =
      case Config.output_less?(opts) do
        true ->
          IO.read(:stdio, :line)

        false ->
          IO.puts(prompt)
          IO.read(:stdio, :line)
      end

    normalize_line(val)
  end

  def consume_multiline_string() do
    normalize_line(IO.read(:stdio, :eof))
  end

  def infer_password(prompt, opts) do
    prompt = if Config.output_less?(opts), do: nil, else: prompt
    normalize_line(read_password_line(prompt))
  end

  @doc false
  def normalize_line(:eof), do: :eof
  def normalize_line({:error, _reason}), do: :eof
  # get_password/0 can return a charlist and pre-strips the line terminator, unlike IO.read/2.
  def normalize_line(data), do: data |> IO.chardata_to_string() |> String.trim()

  # io:get_password/0 (OTP 28.0+) suppresses echo; prompt is printed only right before its read, so a fast sender can't beat raw mode.
  defp read_password_line(prompt) do
    cond do
      masking_available?() ->
        case :shell.start_interactive({:noshell, :raw}) do
          :ok ->
            try do
              maybe_puts_raw(prompt)
              :io.get_password()
            after
              :shell.start_interactive({:noshell, :cooked})
            end

          {:error, _reason} ->
            if stdin_is_terminal?(), do: warn_echoing(:masking_unavailable)
            maybe_puts(prompt)
            IO.read(:stdio, :line)
        end

      function_exported?(:io, :get_password, 0) ->
        if stdin_is_terminal?(), do: warn_echoing(:masking_unavailable)
        maybe_puts(prompt)
        IO.read(:stdio, :line)

      true ->
        if stdin_is_terminal?(), do: warn_echoing(:old_otp)
        maybe_puts(prompt)
        IO.read(:stdio, :line)
    end
  end

  defp maybe_puts(nil), do: :ok
  defp maybe_puts(prompt), do: IO.puts(prompt)

  defp maybe_puts_raw(nil), do: :ok
  # Raw mode disables the driver's LF-to-CRLF translation, so write \r\n explicitly.
  defp maybe_puts_raw(prompt), do: IO.write(prompt <> "\r\n")

  # io:get_password/0 reads from `user`, bypassing a substituted group leader (e.g. tests).
  defp masking_available?() do
    function_exported?(:io, :get_password, 0) and
      Process.group_leader() == Process.whereis(:user)
  end

  # `terminal` reflects stdout, not stdin; prefer `stdin` (OTP 27.0+), falling back below that.
  defp stdin_is_terminal?() do
    case :io.getopts(:standard_io) do
      opts when is_list(opts) ->
        case Keyword.fetch(opts, :stdin) do
          {:ok, stdin} -> stdin == true
          :error -> Keyword.get(opts, :terminal, false) == true
        end

      _ ->
        false
    end
  end

  defp warn_echoing(:old_otp) do
    warn_once(
      "Warning: this Erlang/OTP version cannot mask terminal input, the value typed below will be echoed. Upgrade to Erlang/OTP 28.0 or later to mask password and passphrase prompts."
    )
  end

  defp warn_echoing(:masking_unavailable) do
    warn_once(
      "Warning: terminal input cannot be masked here, the value typed below will be echoed."
    )
  end

  # Guards against printing this once per prompt when a command asks for more than one secret.
  defp warn_once(message) do
    if Process.get(:rabbitmqctl_masking_warned) != true do
      Process.put(:rabbitmqctl_masking_warned, true)
      IO.puts(:stderr, message)
    end
  end
end
