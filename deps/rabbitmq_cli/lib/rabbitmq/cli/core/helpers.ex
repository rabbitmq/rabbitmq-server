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


# Small helper functions, mostly related to connecting to RabbitMQ and
# handling memory units.
alias RabbitMQ.CLI.Core.Config, as: Config

defmodule RabbitMQ.CLI.Core.Helpers do
  require Record

  def get_rabbit_hostname() do
    parse_node(RabbitMQ.CLI.Core.Config.get_option(:node))
  end

  def parse_node(nil), do: get_rabbit_hostname
  def parse_node(name) when is_atom(name) do
    parse_node(to_string(name))
  end
  def parse_node(name) do
    case String.split(name, "@", parts: 2) do
      [_,""] -> name <> "#{hostname}" |> String.to_atom
      [_,_] -> name |> String.to_atom
      [_] -> name <> "@#{hostname}" |> String.to_atom
    end
  end

  def connect_to_rabbitmq, do:        :net_kernel.connect_node(get_rabbit_hostname)
  def connect_to_rabbitmq(input) when is_atom(input), do: :net_kernel.connect_node(input)
  def connect_to_rabbitmq(input) when is_binary(input) do
    input
    |> String.to_atom
    |> :net_kernel.connect_node
  end

  def hostname, do: :inet.gethostname() |> elem(1) |> List.to_string

  def memory_units do
    ["k", "kiB", "M", "MiB", "G", "GiB", "kB", "MB", "GB", ""]
  end

  def memory_unit_absolute(num, unit) when is_number(num) and num < 0, do: {:bad_argument, [num, unit]}

  def memory_unit_absolute(num, "k") when is_number(num),   do: power_as_int(num, 2, 10)
  def memory_unit_absolute(num, "kiB") when is_number(num), do: power_as_int(num, 2, 10)
  def memory_unit_absolute(num, "M") when is_number(num),   do: power_as_int(num, 2, 20)
  def memory_unit_absolute(num, "MiB") when is_number(num), do: power_as_int(num, 2, 20)
  def memory_unit_absolute(num, "G") when is_number(num),   do: power_as_int(num, 2, 30)
  def memory_unit_absolute(num, "GiB") when is_number(num), do: power_as_int(num, 2, 30)
  def memory_unit_absolute(num, "kB") when is_number(num),  do: power_as_int(num, 10, 3)
  def memory_unit_absolute(num, "MB") when is_number(num),  do: power_as_int(num, 10, 6)
  def memory_unit_absolute(num, "GB") when is_number(num),  do: power_as_int(num, 10, 9)
  def memory_unit_absolute(num, "") when is_number(num), do: num
  def memory_unit_absolute(num, unit) when is_number(num), do: {:bad_argument, [unit]}
  def memory_unit_absolute(num, unit), do: {:bad_argument, [num, unit]}

  def power_as_int(num, x, y), do: round(num * (:math.pow(x, y)))

  def nodes_in_cluster(node, timeout \\ :infinity) do
    case :rpc.call(node, :rabbit_mnesia, :cluster_nodes, [:running], timeout) do
      {:badrpc, _} = err -> throw(err);
      value              -> value
    end
  end

  def plugins_dir(opts) do
    case Config.get_option(:plugins_dir, opts) do
      nil -> {:error, :no_plugins_dir};
      dir ->
        paths = String.split(to_string(dir), separator())
        case Enum.any?(paths, &File.dir?/1) do
          true  -> {:ok, dir};
          false -> {:error, :plugins_dir_does_not_exist}
        end
    end
  end

  def require_rabbit(opts) do
    with :ok <- try_load_rabbit_code(opts),
         :ok <- try_load_rabbit_plugins(opts),
         do: :ok
  end

  defp try_load_rabbit_code(opts) do
    home = Config.get_option(:rabbitmq_home, opts)
    case home do
      nil ->
        {:error, {:unable_to_load_rabbit, :rabbitmq_home_is_undefined}};
      _   ->
        path = Path.join(home, "ebin")
        Code.append_path(path)
        case Application.load(:rabbit) do
          :ok ->
            Code.ensure_loaded(:rabbit_plugins)
            :ok;
          {:error, {:already_loaded, :rabbit}} ->
            Code.ensure_loaded(:rabbit_plugins)
            :ok;
          {:error, err} ->
            {:error, {:unable_to_load_rabbit, err}}
        end
    end
  end

  defp try_load_rabbit_plugins(opts) do
    with {:ok, plugins_dir} <- plugins_dir(opts)
    do
      String.split(to_string(plugins_dir), separator())
      |>
      Enum.map(&try_load_plugins_from_directory/1)
      :ok
    end
  end

  defp try_load_plugins_from_directory(directory_with_plugins_inside_it) do
    with {:ok, files} <- File.ls(directory_with_plugins_inside_it)
    do
        Enum.filter_map(files,
            fn(filename) -> String.ends_with?(filename, [".ez"]) end,
          fn(archive) ->
            ## Check that the .app file is present and take the app name from there
            {:ok, ez_files} = :zip.list_dir(String.to_charlist(Path.join([directory_with_plugins_inside_it, archive])))
            case find_dot_app(ez_files) do
              :not_found -> :ok
              dot_app ->
                app_name = Path.basename(dot_app, ".app")
                ebin_dir = Path.join([directory_with_plugins_inside_it, Path.dirname(dot_app)])
                ebin_dir |> Code.append_path()
                app_name |> String.to_atom() |> Application.load()
            end
          end)
    end
  end

  defp find_dot_app([head | tail]) when Record.is_record(head, :zip_file) do
    name = :erlang.element(2, head)
    case Regex.match?(~r/(.+)\/ebin\/(.+)\.app$/, to_string name) do
      true ->
        name
      false ->
        find_dot_app(tail)
    end
  end
  defp find_dot_app([head | tail]) do
    find_dot_app(tail)
  end
  defp find_dot_app([]) do
    :not_found
  end

  def require_mnesia_dir(opts) do
    case Application.get_env(:mnesia, :dir) do
      nil ->
        case Config.get_option(:mnesia_dir, opts) do
          nil -> {:error, :mnesia_dir_not_found};
          val -> Application.put_env(:mnesia, :dir, to_char_list(val))
        end
      _   -> :ok
    end
  end

  def node_running?(node) do
    :net_adm.ping(node) == :pong
  end

  # Convert function to stream
  def defer(fun) do
    Stream.iterate(:ok, fn(_) -> fun.() end)
    |> Stream.drop(1)
    |> Stream.take(1)
  end

  def apply_if_exported(mod, fun, args, default) do
    case function_exported?(mod, fun, length(args)) do
      true  -> apply(mod, fun, args);
      false -> default
    end
  end

  def separator() do
    case :os.type do
        {:unix, _} ->  ":"
        {:win32, _} -> ";"
    end
  end

end
