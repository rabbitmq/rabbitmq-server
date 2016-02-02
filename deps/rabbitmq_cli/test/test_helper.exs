ExUnit.start()

defmodule TestHelper do
  def get_rabbit_hostname() do
   "rabbit@" <> hostname() |> String.to_atom()
  end

  def hostname() do
    elem(:inet.gethostname,1) |> List.to_string()
  end
end
