defmodule ElixirMQCtl do
  import Parser
  import Helpers
  import StatusCommand

  def main(command) do
    :net_kernel.start([:elixirmqctl, :shortnames])

    {parsed_cmd, options} = parse(command)

    case options[:node] do
      nil -> connect_to_rabbitmq |> IO.puts
      _   -> options[:node] |> String.to_atom |> connect_to_rabbitmq |> IO.puts
    end

    run_command(parsed_cmd, options)
    :net_kernel.stop()
  end

  defp print_usage() do
    IO.puts "Usage: TBD"
  end

  defp print_nodedown_error(options) do
    target_node = options[:node] || get_rabbit_hostname

    IO.puts "Status of #{target_node} ..."
    IO.puts "Error: unable to connect to node '#{target_node}': nodedown"
  end

  defp run_command([], _), do: IO.puts print_usage
  defp run_command(["status"], options) do
    case result = status(options) do
      {:badrpc, :nodedown}  -> print_nodedown_error(options)
      _                     -> print_status(result)
    end
  end
end
