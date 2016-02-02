ExUnit.start()

defmodule TestHelper do
  def get_rabbit_hostname() do
   "rabbit@" <> hostname() |> String.to_atom()
  end

  def hostname() do
    elem(:inet.gethostname,1) |> List.to_string()
  end

  def pid_string(pid) when is_pid(pid) do
    :io_lib.format("~p", [pid]) |> 
    List.to_string() |> 
    String.slice(1..-2) |> 
    String.replace(".", "")
  end

  def unique_node_name(prefix) when is_binary(prefix) do
    prefix <> pid_string(self()) |> String.to_atom()
  end
end
