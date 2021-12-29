## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.InfoKeys do
  import RabbitCommon.Records
  alias RabbitMQ.CLI.Core.DataCoercion

  def validate_info_keys(args, valid_keys) do
    info_keys = prepare_info_keys(args)

    case invalid_info_keys(info_keys, Enum.map(valid_keys, &DataCoercion.to_atom/1)) do
      [_ | _] = bad_info_keys ->
        {:validation_failure, {:bad_info_key, bad_info_keys}}

      [] ->
        {:ok, info_keys}
    end
  end

  def prepare_info_keys(args) do
    args
    |> Enum.flat_map(fn arg -> String.split(arg, ",", trim: true) end)
    |> Enum.map(fn s -> String.replace(s, ",", "") end)
    |> Enum.map(&String.trim/1)
    |> Enum.map(&String.to_atom/1)
    |> Enum.uniq()
  end

  def with_valid_info_keys(args, valid_keys, fun) do
    case validate_info_keys(args, valid_keys) do
      {:ok, info_keys} -> fun.(info_keys)
      err -> err
    end
  end

  defp invalid_info_keys(info_keys, valid_keys) do
    MapSet.new(info_keys)
    |> MapSet.difference(MapSet.new(valid_keys))
    |> MapSet.to_list()
  end

  def info_for_keys(item, []) do
    item
  end

  def info_for_keys([{_, _} | _] = item, info_keys) do
    item
    |> Enum.filter(fn {k, _} -> Enum.member?(info_keys, k) end)
    |> Enum.map(fn {k, v} -> {k, format_info_item(v)} end)
  end

  defp format_info_item(resource(name: name)) do
    name
  end

  defp format_info_item(any) do
    any
  end
end
