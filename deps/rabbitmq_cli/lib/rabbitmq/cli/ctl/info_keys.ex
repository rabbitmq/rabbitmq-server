## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.InfoKeys do
  import RabbitCommon.Records
  alias RabbitMQ.CLI.Core.DataCoercion

  # internal to requested keys
  @type info_keys :: [atom | tuple]
  # requested to internal keys
  @type aliases :: keyword(atom)

  def validate_info_keys(args, valid_keys) do
    validate_info_keys(args, valid_keys, [])
  end

  @spec validate_info_keys([charlist], [charlist | atom], aliases) ::
          {:ok, info_keys} | {:validation_failure, any}
  def validate_info_keys(args, valid_keys, aliases) do
    info_keys = prepare_info_keys(args, aliases)

    case invalid_info_keys(info_keys, Enum.map(valid_keys, &DataCoercion.to_atom/1)) do
      [_ | _] = bad_info_keys ->
        {:validation_failure, {:bad_info_key, bad_info_keys}}

      [] ->
        {:ok, info_keys}
    end
  end

  def prepare_info_keys(args) do
    prepare_info_keys(args, [])
  end

  @spec prepare_info_keys([charlist], aliases) :: info_keys
  def prepare_info_keys(args, aliases) do
    args
    |> Enum.flat_map(fn arg -> String.split(arg, ",", trim: true) end)
    |> Enum.map(fn s -> String.replace(s, ",", "") end)
    |> Enum.map(&String.trim/1)
    |> Enum.map(&String.to_atom/1)
    |> Enum.map(fn k ->
      case Keyword.get(aliases, k) do
        nil -> k
        v -> {v, k}
      end
    end)
    |> Enum.uniq()
    |> :proplists.compact()
  end

  def broker_keys(info_keys) do
    Enum.map(
      info_keys,
      fn
        {k, _} -> k
        k -> k
      end
    )
  end

  def with_valid_info_keys(args, valid_keys, fun) do
    with_valid_info_keys(args, valid_keys, [], fun)
  end

  @spec with_valid_info_keys([charlist], [charlist], aliases, ([atom] -> any)) :: any
  def with_valid_info_keys(args, valid_keys, aliases, fun) do
    case validate_info_keys(args, valid_keys, aliases) do
      {:ok, info_keys} -> fun.(:proplists.get_keys(info_keys))
      err -> err
    end
  end

  @spec invalid_info_keys(info_keys, [atom]) :: [atom]
  defp invalid_info_keys(info_keys, valid_keys) do
    info_keys
    |> :proplists.get_keys()
    |> MapSet.new()
    |> MapSet.difference(MapSet.new(valid_keys))
    |> MapSet.to_list()
    |> Enum.map(fn k ->
      case :proplists.get_value(k, info_keys, k) do
        true -> k
        v -> v
      end
    end)
  end

  @spec info_for_keys(keyword, info_keys) :: keyword

  def info_for_keys(item, []) do
    item
  end

  def info_for_keys([{_, _} | _] = item, info_keys) do
    item
    |> Enum.filter(fn {k, _} -> :proplists.is_defined(k, info_keys) end)
    |> Enum.map(fn {k, v} ->
      original =
        case :proplists.get_value(k, info_keys) do
          true -> k
          v -> v
        end

      {original, format_info_item(v)}
    end)
  end

  defp format_info_item(resource(name: name)) do
    name
  end

  defp format_info_item(any) do
    any
  end
end
