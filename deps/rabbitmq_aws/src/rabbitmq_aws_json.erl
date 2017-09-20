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
decode(Value) ->
  Decoded = rabbit_json:decode(Value),
  convert_binary_values(Decoded).

-spec convert_binary_values
        (Value :: map()) -> map();
        (Value :: list()) -> list();
        (Value :: binary()) -> list();
        (Value :: atom()) -> atom();
        (Value :: integer()) -> integer().
%% @doc Convert the binary key/value pairs returned by JSX to strings.
%% @end
convert_binary_values(Map) when is_map(Map) ->
    maps:fold(fun convert_binary_values/3, #{}, Map);
convert_binary_values(List) when is_list(List) ->
    lists:map(fun convert_binary_values/1, List);
convert_binary_values(Binary) when is_binary(Binary) ->
    binary_to_list(Binary);
convert_binary_values(Other) ->
    Other.

convert_binary_values(Key, Value, Map) ->
    Key1 = convert_binary_values(Key),
    Value1 = convert_binary_values(Value),
    Map#{Key1 => Value1}.
