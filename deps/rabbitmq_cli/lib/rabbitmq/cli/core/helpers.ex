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
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.


# Small helper functions, mostly related to connecting to RabbitMQ and
# handling memory units.
alias RabbitMQ.CLI.Core.Config

defmodule RabbitMQ.CLI.Core.Helpers do
  require Record

  def get_rabbit_hostname() do
    parse_node(RabbitMQ.CLI.Core.Config.get_option(:node))
  end

  def parse_node(nil), do: get_rabbit_hostname()
  def parse_node(name) when is_atom(name) do
    parse_node(to_string(name))
  end
  def parse_node(name) do
    case String.split(name, "@", parts: 2) do
      [_,""] -> name <> "#{hostname()}" |> String.to_atom
      [_,_] -> name |> String.to_atom
      [_] -> name <> "@#{hostname()}" |> String.to_atom
    end
  end

  def hostname, do: :inet.gethostname() |> elem(1) |> List.to_string

  def validate_step(:ok, step) do
    case step.() do
      {:error, err} -> {:validation_failure, err};
      _             -> :ok
    end
  end
  def validate_step({:validation_failure, err}, _) do
    {:validation_failure, err}
  end

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
    with_nodes_in_cluster(node, fn(nodes) -> nodes end, timeout)
  end

  def with_nodes_in_cluster(node, fun, timeout \\ :infinity) do
    case :rpc.call(node, :rabbit_mnesia, :cluster_nodes, [:running], timeout) do
      {:badrpc, _} = err -> err
      value              -> fun.(value)
    end
  end

  def plugins_dir(_, opts) do
    plugins_dir(opts)
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

  def require_rabbit_and_plugins(_, opts) do
    require_rabbit_and_plugins(opts)
  end

  def require_rabbit_and_plugins(opts) do
    with :ok <- require_rabbit(opts),
         :ok <- add_plugins_to_load_path(opts),
         do: :ok
  end

  def require_rabbit(_, opts) do
    require_rabbit(opts)
  end

  def require_rabbit(opts) do
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

  def rabbit_app_running?(%{node: node, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node, :rabbit, :is_running, [], timeout) do
      true  -> true
      false -> false
      other -> {:error, other}
    end
  end

  def rabbit_app_running?(_, opts) do
    rabbit_app_running?(opts)
  end

  def add_plugins_to_load_path(opts) do
    with {:ok, plugins_dir} <- plugins_dir(opts)
    do
      String.split(to_string(plugins_dir), separator())
      |>
      Enum.map(&add_directory_plugins_to_load_path/1)
      :ok
    end
  end

  def add_directory_plugins_to_load_path(directory_with_plugins_inside_it) do
    with {:ok, files} <- File.ls(directory_with_plugins_inside_it)
    do
      Enum.map(files,
        fn(filename) ->
          cond do
            String.ends_with?(filename, [".ez"]) ->
              Path.join([directory_with_plugins_inside_it, filename])
              |> String.to_charlist
              |> add_archive_code_path();
            File.dir?(filename) ->
              Path.join([directory_with_plugins_inside_it, filename])
              |> add_dir_code_path();
            true ->
              {:error, {:not_a_plugin, filename}}
          end
        end)
    end
  end

  defp add_archive_code_path(ez_dir) do
    case :erl_prim_loader.list_dir(ez_dir) do
      {:ok, [app_dir]} ->
        app_in_ez = :filename.join(ez_dir, app_dir)
        add_dir_code_path(app_in_ez);
      _ -> {:error, :no_app_dir}
    end
  end

  defp add_dir_code_path(app_dir_0) do
    app_dir = to_charlist(app_dir_0)
    case :erl_prim_loader.list_dir(app_dir) do
      {:ok, list} ->
        case Enum.member?(list, 'ebin') do
          true ->
            ebin_dir = :filename.join(app_dir, 'ebin')
            Code.append_path(ebin_dir)
          false -> {:error, :no_ebin}
        end;
      _ -> {:error, :app_dir_empty}
    end
  end

  def require_mnesia_dir(opts) do
    case Application.get_env(:mnesia, :dir) do
      nil ->
        case Config.get_option(:mnesia_dir, opts) do
          nil -> {:error, :mnesia_dir_not_found};
          val -> Application.put_env(:mnesia, :dir, to_charlist(val))
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

  # Streamify a function sequence passing result
  # Execution can be terminated by an error {:error, _}.
  # The error will be the last element in the stream.
  # Functions can return {:ok, val}, so val will be passed
  # to then next function, or {:ok, val, output} where
  # val will be passed and output will be put into the stream
  def stream_until_error_parametrised(funs, init) do
    Stream.transform(
      funs, {:just, init},
      fn(f, {:just, val}) ->
          case f.(val) do
            {:error, _} = err -> {[err], :nothing};
            :ok               -> {[], {:just, val}};
            {:ok, new_val}        -> {[], {:just, new_val}};
            {:ok, new_val, out}   -> {[out], {:just, new_val}}
          end;
        (_, :nothing) ->
          {:halt, :nothing}
      end)
  end

  # Streamify function sequence.
  # Execution can be terminated by an error {:error, _}.
  # The error will be the last element in the stream.
  def stream_until_error(funs) do
    stream_until_error_parametrised(
      Enum.map(
        funs,
        fn(fun) ->
          fn(:no_param) ->
            case fun.() do
              {:error, _} = err -> err;
              other             -> {:ok, :no_param, other}
            end
          end
        end),
      :no_param)
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

  def cli_acting_user, do: "rmq-cli"

  def string_or_inspect(val) do
    case String.Chars.impl_for(val) do
      nil -> inspect(val);
      _   ->
        try do to_string(val)
        catch _, _ -> inspect(val)
        end
    end
  end

  def evaluate_input_as_term(input) do
      {:ok,tokens,_end_line} = :erl_scan.string(to_charlist(input <> "."))
      {:ok,abs_form} = :erl_parse.parse_exprs(tokens)
      {:value,term_value,_bs} = :erl_eval.exprs(abs_form, :erl_eval.new_bindings())
      term_value
  end
end
