defmodule StatusCommand do
  import Helpers

  @id_length 10
  @name_length 16

  def status(options) do
    case options[:node] do
      nil -> get_rabbit_hostname |> :rpc.call(:rabbit, :status, [])
      host when is_atom(host) -> host |> :rpc.call(:rabbit, :status, [])
      host when is_binary(host) -> host |> String.to_atom() |> :rpc.call(:rabbit, :status, [])
    end
  end

  def print_status(result) do
    result 
    |> print_pid
    |> print_running_apps
  end

  defp print_pid(result) when not is_list(result), do: result
  defp print_pid(result) when is_list(result) do
    case result[:pid] do
      nil -> nil
      _ -> IO.puts "PID: #{result[:pid]}"
    end
    result
  end

  defp print_running_apps(result) when not is_list(result), do: result
  defp print_running_apps(result) when is_list(result) do
    IO.puts "Applications currently running:"
    IO.puts "---------------------------------------"

    case result[:running_applications] do
      nil -> nil
      _ ->  result[:running_applications] |> Enum.map(
              fn ({id, name, version}) ->
                :io.format(
                  "~-#{@id_length}s | ~-#{@name_length}s | ~s\n", 
                  [id, name, version]
                )
              end
            )
    end
    result
  end
end
