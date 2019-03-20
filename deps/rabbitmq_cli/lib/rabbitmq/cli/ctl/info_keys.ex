## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.InfoKeys do
  import RabbitCommon.Records

  def validate_info_keys(args, valid_keys) do
    info_keys = prepare_info_keys(args)

    case invalid_info_keys(info_keys, valid_keys) do
      [_ | _] = bad_info_keys ->
        {:validation_failure, {:bad_info_key, bad_info_keys}}

      [] ->
        {:ok, info_keys}
    end
  end

  def prepare_info_keys(args) do
    args
    |> Enum.flat_map(fn arg -> String.split(arg, ",", trim: true) end)
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
