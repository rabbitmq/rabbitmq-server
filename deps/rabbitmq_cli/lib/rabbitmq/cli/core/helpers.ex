## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.Helpers do
  alias RabbitMQ.CLI.Core.{Config, DataCoercion, NodeName}
  require Record

  def get_rabbit_hostname(node_name_type \\ :shortnames) do
    normalise_node(Config.get_option(:node), node_name_type)
  end

  def normalise_node(nil, node_name_type) do
    normalise_node(Config.get_option(:node), node_name_type)
  end

  def normalise_node(name, node_name_type) do
    case NodeName.create(name, node_name_type) do
      {:ok, node_name} -> node_name
      other            -> other
    end
  end

  # rabbitmq/rabbitmq-cli#278
  def normalise_node_option(options) do
    node_opt = Config.get_option(:node, options)
    longnames_opt = Config.get_option(:longnames, options)
    case NodeName.create(node_opt, longnames_opt) do
      {:error, _} = err ->
        err
      {:ok, val} ->
        {:ok, Map.put(options, :node, val)}
    end
  end

  def normalise_node_option(nil, _, _) do
    nil
  end
  def normalise_node_option(node_opt, longnames_opt, options) do
    case NodeName.create(node_opt, longnames_opt) do
      {:error, _} = err ->
        err
      {:ok, val} ->
        {:ok, Map.put(options, :node, val)}
    end
  end

  def case_insensitive_format(%{format: format} = opts) do
    %{opts | format: String.downcase(format)}
  end
  def case_insensitive_format(opts), do: opts

  def nodes_in_cluster(node, timeout \\ :infinity) do
    with_nodes_in_cluster(node, fn nodes -> nodes end, timeout)
  end

  def with_nodes_in_cluster(node, fun, timeout \\ :infinity) do
    case :rpc.call(node, :rabbit_mnesia, :cluster_nodes, [:running], timeout) do
      {:badrpc, _} = err -> err
      value -> fun.(value)
    end
  end

  def node_running?(node) do
    :net_adm.ping(node) == :pong
  end

  # Convert function to stream
  def defer(fun) do
    Stream.iterate(:ok, fn _ -> fun.() end)
    |> Stream.drop(1)
    |> Stream.take(1)
  end

  # Streamify a function sequence passing result
  # Execution can be terminated by an error {:error, _}.
  # The error will be the last element in the stream.
  # Functions can return {:ok, val}, so val will be passed
  # to then next function, or {:ok, val, output} where
  # val will be passed and output will be put into the stream
  def stream_until_error_parameterised(funs, init) do
    Stream.transform(funs, {:just, init}, fn
      f, {:just, val} ->
        case f.(val) do
          {:error, _} = err -> {[err], :nothing}
          :ok -> {[], {:just, val}}
          {:ok, new_val} -> {[], {:just, new_val}}
          {:ok, new_val, out} -> {[out], {:just, new_val}}
        end

      _, :nothing ->
        {:halt, :nothing}
    end)
  end

  # Streamify function sequence.
  # Execution can be terminated by an error {:error, _}.
  # The error will be the last element in the stream.
  def stream_until_error(funs) do
    stream_until_error_parameterised(
      Enum.map(
        funs,
        fn fun ->
          fn :no_param ->
            case fun.() do
              {:error, _} = err -> err
              other -> {:ok, :no_param, other}
            end
          end
        end
      ),
      :no_param
    )
  end

  def atomize_values(map, keys) do
    Enum.reduce(map, %{},
                fn({k, v}, acc) ->
                  case Enum.member?(keys, k) do
                    false -> Map.put(acc, k, v)
                    true  -> Map.put(acc, k, DataCoercion.to_atom(v))
                  end
                end)
  end

  def apply_if_exported(mod, fun, args, default) do
    Code.ensure_loaded(mod)
    case function_exported?(mod, fun, length(args)) do
      true -> apply(mod, fun, args)
      false -> default
    end
  end

  def cli_acting_user, do: "rmq-cli"

  def string_or_inspect(val) do
    case String.Chars.impl_for(val) do
      nil ->
        inspect(val)

      _ ->
        try do
          to_string(val)
        catch
          _, _ -> inspect(val)
        end
    end
  end

  def evaluate_input_as_term(input) do
    {:ok, tokens, _end_line} = :erl_scan.string(to_charlist(input <> "."))
    {:ok, abs_form} = :erl_parse.parse_exprs(tokens)
    {:value, term_value, _bs} = :erl_eval.exprs(abs_form, :erl_eval.new_bindings())
    term_value
  end
end
