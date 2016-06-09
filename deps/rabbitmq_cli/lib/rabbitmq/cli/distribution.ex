
defmodule RabbitMQ.CLI.Distribution do

  def start() do
    start(%{})
  end

  def start(options) do
    names_opt = case options[:longnames] do
      true  -> [:longnames];
      false -> [:shortnames];
      nil   -> [:shortnames]
    end
    start(names_opt, 10, :undefined)
  end  

  defp start(_opt, 0, last_err) do
    {:error, last_err}
  end

  defp start(names_opt, attempts, _last_err) do
    candidate = String.to_atom("rabbitmqcli" <>
                               to_string(:rabbit_misc.random(100)))
    case :net_kernel.start([candidate | names_opt]) do
      {:ok, _} = ok    -> ok;
      {:error, reason} -> start(names_opt, attempts - 1, reason)
    end
  end

end
