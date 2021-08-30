%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @doc Wrap a JSON parser to provide easy abstractions across
%%      implementations and ensure a consistent return interface.
%% @end
%% ====================================================================
-module(rabbitmq_aws_json).

-export([decode/1]).

-spec decode(Value :: string() | binary()) -> list().
%% @doc Decode a JSON string returning a proplist
%% @end
decode(Value) when is_list(Value) ->
  decode(list_to_binary(Value));
decode(Value) when is_binary(Value) ->
  Decoded0 = rabbit_json:decode(Value, [{return_maps, false}]),
  Decoded  = rabbit_data_coercion:to_proplist(Decoded0),
  convert_binary_values(Decoded, []).


-spec convert_binary_values(Value :: list(), Accumulator :: list()) -> list().
%% @doc Convert the binary key/value pairs returned by rabbit_json to strings.
%% @end
convert_binary_values([], Value) ->  Value;
convert_binary_values([{K, V}|T], Accum) when is_map(V) ->
  convert_binary_values(
    T,
    lists:append(
      Accum,
      [{binary_to_list(K), convert_binary_values(maps:to_list(V), [])}]));
convert_binary_values([{K, V}|T], Accum) when is_list(V) ->
  convert_binary_values(
    T,
    lists:append(
      Accum,
      [{binary_to_list(K), convert_binary_values(V, [])}]));
convert_binary_values([{}|T],Accum) ->
  convert_binary_values(T, lists:append(Accum, [{}]));
convert_binary_values([{K, V}|T], Accum) when is_binary(V) ->
  convert_binary_values(T, lists:append(Accum, [{binary_to_list(K), binary_to_list(V)}]));
convert_binary_values([{K, V}|T], Accum) ->
  convert_binary_values(T, lists:append(Accum, [{binary_to_list(K), V}]));
convert_binary_values([H|T], Accum) when is_map(H) ->
  convert_binary_values(T, lists:append(Accum, convert_binary_values(maps:to_list(H), [])));
convert_binary_values([H|T], Accum) when is_binary(H) ->
  convert_binary_values(T, lists:append(Accum, [binary_to_list(H)]));
convert_binary_values([H|T], Accum) when is_integer(H) ->
  convert_binary_values(T, lists:append(Accum, [H]));
convert_binary_values([H|T], Accum) when is_atom(H) ->
  convert_binary_values(T, lists:append(Accum, [H]));
convert_binary_values([H|T], Accum) ->
  convert_binary_values(T, lists:append(Accum, convert_binary_values(H, []))).
