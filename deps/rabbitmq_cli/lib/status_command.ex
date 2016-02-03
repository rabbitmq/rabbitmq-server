## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


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
