## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Core.Helpers do
  alias RabbitMQ.CLI.Core.{Config, NodeName}
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
      {:ok, normalised_node_opt} ->
        {:ok, Map.put(options, :node, normalised_node_opt)}
    end
  end

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
