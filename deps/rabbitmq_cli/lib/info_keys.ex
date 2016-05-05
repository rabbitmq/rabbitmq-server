defmodule InfoKeys do
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
        for key <- info_keys, not Enum.member?(valid_keys, key), do: key
    end
end