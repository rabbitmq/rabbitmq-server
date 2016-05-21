defmodule InfoKeys do
  require Record

  Record.defrecord(:resource, :resource,
    Record.extract(:resource,
      from_lib: "rabbit_common/include/rabbit.hrl"))

  def with_valid_info_keys(args, valid_keys, fun) do
    info_keys = Enum.map(args, &String.to_atom/1)
    case invalid_info_keys(info_keys, valid_keys) do
      [_|_] = bad_info_keys ->
        {:error, {:bad_info_key, bad_info_keys}}
      [] -> fun.(info_keys)
    end
  end

  defp invalid_info_keys(info_keys, valid_keys) do
    # Difference between enums.
    # It's faster than converting to sets for small lists
    info_keys -- valid_keys
  end

  def info_for_keys(item, []) do
    item
  end

  def info_for_keys([{_,_}|_] = item, info_keys) do
    Enum.filter_map(item,
      fn({k, _}) -> Enum.member?(info_keys, k) end,
      fn({k, v}) -> {k, format_info_item(v)} end)
  end

  defp format_info_item(res) when Record.is_record(res, :resource) do
    resource(res, :name)
  end

  defp format_info_item(any) do
    any
  end
end
