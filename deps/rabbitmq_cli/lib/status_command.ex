defmodule StatusCommand do
  import Helpers

  @id_length 10
  @name_length 16

  def status(options) do
    case options[:node] do
      nil -> :rpc.call(get_rabbit_hostname(), :rabbit, :status, [])
      host when is_atom(host) -> :rpc.call(host, :rabbit, :status, [])
      host when is_binary(host) -> :rpc.call(String.to_atom(host), :rabbit, :status, [])
    end
  end

  def print_status(result) do
    result |> 
      print_pid |>
      print_running_apps
  end

  defp print_pid(result) when is_list(result) do
    case result[:pid] do
      nil -> nil
      _ -> IO.puts "PID: #{result[:pid]}"
    end
    result
  end

  defp print_pid(result) when not is_list(result) do
    result
  end

  defp print_running_apps(result) when is_list(result) do
    IO.puts "Applications currently running:"
    IO.puts "---------------------------------------"

    case result[:running_applications] do
      nil -> nil
      _ -> Enum.map(
              result[:running_applications], 
              fn ({id, name, version}) -> :io.format("~-#{@id_length}s | ~-#{@name_length}s | ~s\n", [id, name, version])
              end
           )
    end
    result
  end

  defp print_running_apps(result) when not is_list(result) do
    result
  end
end
