%% ====================================================================
%% @author Gavin M. Roy <gavinmroy@gmail.com>
%% @copyright 2016, Gavin M. Roy
%% @doc Wrap a JSON parser to provide easy abstractions across
%%      implementations and ensure a consistent return interface.
%% @end
%% ====================================================================
-module(httpc_aws_json).

-export([decode/1]).

-spec decode(Value :: string() | binary()) -> list().
%% @doc Decode a JSON string returning a proplist
%% @end
decode(Value) when is_list(Value) ->
  convert_binary_values(jsx:decode(list_to_binary(Value)), []);
decode(Value) ->
  convert_binary_values(jsx:decode(Value), []).

-spec convert_binary_values(Value :: atom() | binary() | integer() | list(),
                            Accumulator :: list()) -> list().
%% @doc Convert the binary key/value pairs returned by JSX to strings.
%% @end
convert_binary_values([], Value) ->  Value;
convert_binary_values([{K, V}|T], Accum) when is_list(V) ->
  convert_binary_values(T, lists:append(Accum, [{binary_to_list(K), convert_binary_values(V, [])}]));
convert_binary_values([{K, V}|T], Accum) when is_binary(V) ->
  convert_binary_values(T, lists:append(Accum, [{binary_to_list(K), binary_to_list(V)}]));
convert_binary_values([{K, V}|T], Accum) ->
  convert_binary_values(T, lists:append(Accum, [{binary_to_list(K), V}]));
convert_binary_values([H|T], Accum) when is_binary(H) ->
convert_binary_values(T, lists:append(Accum, [binary_to_list(H)]));
convert_binary_values([H|T], Accum) when is_integer(H) ->
convert_binary_values(T, lists:append(Accum, [H]));
convert_binary_values([H|T], Accum) when is_atom(H) ->
convert_binary_values(T, lists:append(Accum, [H]));
convert_binary_values([H|T], Accum) ->
  convert_binary_values(T, lists:append(Accum, convert_binary_values(H, []))).
