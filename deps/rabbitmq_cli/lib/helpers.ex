defmodule Helpers do
  @rabbit_host "rabbit"

  def get_rabbit_hostname(), do: (@rabbit_host <> "@" <> hostname) |> String.to_atom

  def connect_to_rabbitmq(), do:      :net_kernel.connect_node(get_rabbit_hostname)
  def connect_to_rabbitmq(input), do: :net_kernel.connect_node(input)

  defp hostname(), do: :inet.gethostname() |> elem(1) |> List.to_string
end
