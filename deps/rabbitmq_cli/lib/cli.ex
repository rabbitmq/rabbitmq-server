defmodule CLI do
  import Parser
  import Helpers
  import StatusCommand

  def main(command) do
    unless Node.alive?(), do: :net_kernel.start([:rabbitmqctl, :shortnames])

    {parsed_cmd, options} = parse(command)

    case options[:node] do
      nil -> IO.puts connect_to_rabbitmq()
      _ -> IO.puts connect_to_rabbitmq(String.to_atom(options[:node]))
    end

    run_command(parsed_cmd, options)
    :net_kernel.stop()
  end

  defp print_nodedown_error(options) do
    target_node = options[:node] || get_rabbit_hostname()

    IO.puts "Status of #{target_node} ..."
    IO.puts "Error: unable to connect to node '#{target_node}': nodedown"
  end

  defp run_command(["status"], options) do
    case result = status(options) do
      {:badrpc, :nodedown} -> print_nodedown_error(options)
      _ -> print_status(result)
    end
  end
end
