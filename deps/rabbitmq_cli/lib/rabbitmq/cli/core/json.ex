## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Core.JSON do
  @moduledoc """
  Thin JSON facade used by the CLI tools.

  Wraps `:thoas` so the rest of the codebase does not depend on a specific
  backend, and so the module name does not collide with the `JSON` module
  added to Elixir's standard library in 1.18.

  `encode/1` returns `{:ok, binary}` and `decode/1` returns
  `{:ok, term} | {:error, term}`, matching the shape that callers throughout
  `rabbitmq_cli` already rely on.
  """

  @spec encode(term()) :: {:ok, binary()}
  def encode(term) do
    {:ok, :thoas.encode(normalize(term))}
  end

  @spec decode(iodata()) :: {:ok, term()} | {:error, term()}
  def decode(bin) do
    :thoas.decode(bin)
  end

  # Convert Erlang strings (lists of integers) to binaries for proper JSON
  # encoding and convert other Erlang-specific terms to readable strings.
  defp normalize(data) when is_function(data) do
    "Fun()"
  end

  defp normalize(data) when is_pid(data) do
    "Pid(#{inspect(data)})"
  end

  defp normalize(data) when is_port(data) do
    "Port(#{inspect(data)})"
  end

  defp normalize(data) when is_reference(data) do
    "Ref(#{inspect(data)})"
  end

  defp normalize(data) when is_binary(data) do
    convert_binary(data)
  end

  defp normalize([]), do: []

  # Likely a value like [5672], which we don't want to convert to the
  # equivalent unicode codepoint.
  defp normalize([val] = data) when is_integer(val) and val > 255 do
    data
  end

  # Likely a value like [5672, 5682], which we don't want to convert to
  # the equivalent unicode codepoint.
  defp normalize([v0, v1] = data)
       when is_integer(v0) and v0 > 255 and is_integer(v1) and v1 > 255 do
    data
  end

  defp normalize([b | rest]) when is_binary(b) do
    [convert_binary(b) | normalize(rest)]
  end

  defp normalize(data) when is_list(data) do
    if proplist?(data) do
      # `:thoas` encodes maps as JSON objects but is unreliable on lists of
      # 2-tuples that contain non-proplist values nested inside, so we hand it
      # a real map.
      Map.new(data, fn {k, v} -> {normalize(k), normalize(v)} end)
    else
      try do
        case :unicode.characters_to_binary(data, :utf8) do
          binary when is_binary(binary) ->
            binary

          _ ->
            Enum.map(data, &normalize/1)
        end
      rescue
        ArgumentError ->
          Enum.map(data, &normalize/1)
      end
    end
  end

  # `:thoas` does not accept bare tuples (only proplist 2-tuples nested in a
  # list, handled above). Convert any other tuple to a list so it encodes as a
  # JSON array, matching what the previous JSON library used to do.
  defp normalize(data) when is_tuple(data) do
    data
    |> Tuple.to_list()
    |> Enum.map(&normalize/1)
  end

  defp normalize(data) when is_map(data) do
    Enum.into(data, %{}, fn {k, v} -> {normalize(k), normalize(v)} end)
  end

  defp normalize(data), do: data

  defp proplist?([_ | _] = list) do
    Enum.all?(list, fn
      {k, _v} when is_atom(k) or is_binary(k) -> true
      _ -> false
    end)
  end

  defp proplist?(_), do: false

  defp convert_binary(data) when is_binary(data) do
    try do
      case :unicode.characters_to_binary(data, :utf8) do
        binary when is_binary(binary) ->
          binary

        _ ->
          Base.encode64(data)
      end
    rescue
      ArgumentError ->
        Base.encode64(data)
    end
  end
end
