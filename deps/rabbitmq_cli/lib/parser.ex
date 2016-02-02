defmodule Parser do

  def parse(command) do
    {options, cmd, _} = OptionParser.parse(
      command,
      switches: [node: :atom],
      aliases: [n: :node]
    )
    {clear_on_empty_command(cmd), options}
  end

  defp clear_on_empty_command(command_args) do
    case command_args do
      [] -> []
      [""|_] -> []
      [head|_] -> command_args
    end
  end
end
