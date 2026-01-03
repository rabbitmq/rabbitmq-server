%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_data_coercion).

-export([to_binary/1, to_list/1, to_atom/1, to_integer/1, to_boolean/1, to_proplist/1, to_map/1, to_map_recursive/1]).
-export([to_atom/2, atomize_keys/1, to_list_of_binaries/1]).
-export([to_utf8_binary/1, to_unicode_charlist/1]).
-export([as_list/1]).

-spec to_binary(Val :: binary() | list() | atom() | integer() | function()) -> binary().
to_binary(Val) when is_list(Val)     -> list_to_binary(Val);
to_binary(Val) when is_atom(Val)     -> atom_to_binary(Val, utf8);
to_binary(Val) when is_integer(Val)  -> integer_to_binary(Val);
to_binary(Val) when is_function(Val) -> list_to_binary(io_lib:format("~w", [Val]));
to_binary(Val)                       -> Val.

-spec to_list(Val :: integer() | list() | binary() | atom() | map() | inet:ip_address()) -> list().
to_list(Val) when is_list(Val)    -> Val;
to_list(Val) when is_map(Val)     -> maps:to_list(Val);
to_list(Val) when is_atom(Val)    -> atom_to_list(Val);
to_list(Val) when is_binary(Val)  -> binary_to_list(Val);
to_list(Val) when is_integer(Val) -> integer_to_list(Val);
to_list({V0, V1, V2, V3}=Val) when (is_integer(V0) andalso (V0 >=0 andalso V0 =< 255)) andalso
                                   (is_integer(V1) andalso (V1 >=0 andalso V1 =< 255)) andalso
                                   (is_integer(V2) andalso (V2 >=0 andalso V2 =< 255)) andalso
                                   (is_integer(V3) andalso (V3 >=0 andalso V3 =< 255)) ->
    io_lib:format("~w", [Val]);
to_list({V0, V1, V2, V3, V4, V5, V6, V7}=Val)
  when (is_integer(V0) andalso (V0 >=0 andalso V0 =< 65535)) andalso
       (is_integer(V1) andalso (V1 >=0 andalso V1 =< 65535)) andalso
       (is_integer(V2) andalso (V2 >=0 andalso V2 =< 65535)) andalso
       (is_integer(V3) andalso (V3 >=0 andalso V3 =< 65535)) andalso
       (is_integer(V4) andalso (V4 >=0 andalso V4 =< 65535)) andalso
       (is_integer(V5) andalso (V5 >=0 andalso V5 =< 65535)) andalso
       (is_integer(V6) andalso (V6 >=0 andalso V6 =< 65535)) andalso
       (is_integer(V7) andalso (V7 >=0 andalso V7 =< 65535)) ->
    io_lib:format("~w", [Val]).

-spec to_atom(Val :: atom() | list() | binary()) -> atom().
to_atom(Val) when is_atom(Val)   -> Val;
to_atom(Val) when is_list(Val)   -> list_to_atom(Val);
to_atom(Val) when is_binary(Val) -> binary_to_atom(Val, utf8).

-spec to_atom(Val :: atom() | list() | binary(), Encoding :: atom()) -> atom().
to_atom(Val, _Encoding) when is_atom(Val)   -> Val;
to_atom(Val, _Encoding) when is_list(Val)   -> list_to_atom(Val);
to_atom(Val, Encoding)  when is_binary(Val) -> binary_to_atom(Val, Encoding).

-spec to_integer(Val :: integer() | list() | binary()) -> integer().
to_integer(Val) when is_integer(Val) -> Val;
to_integer(Val) when is_list(Val)    -> list_to_integer(Val);
to_integer(Val) when is_binary(Val)  -> binary_to_integer(Val).

-spec to_boolean(Val :: boolean() | list() | binary()) -> boolean().
to_boolean(Val) when is_boolean(Val) -> Val;
to_boolean(Val) when is_list(Val)    -> list_to_boolean(string:lowercase(Val));
to_boolean(Val) when is_binary(Val)  -> list_to_boolean(string:lowercase(binary_to_list(Val))).

-spec to_proplist(Val :: map() | list()) -> list().
to_proplist(Val) when is_list(Val) -> Val;
to_proplist(Val) when is_map(Val) -> maps:to_list(Val).

-spec to_map(Val :: map() | list()) -> map().
to_map(Val) when is_map(Val) -> Val;
to_map(Val) when is_list(Val) -> maps:from_list(Val).

-spec atomize_keys(Val :: map() | list()) -> map() | list().
atomize_keys(Val) when is_list(Val) ->
    [{to_atom(K), V} || {K, V} <- Val];
atomize_keys(Val) when is_map(Val) ->
    #{to_atom(K) => V || K := V <- Val}.

-spec to_list_of_binaries(Val :: undefined | [atom() | list() | binary() | integer()]) -> [binary()].
to_list_of_binaries(Value) ->
    case Value of
      undefined ->
          [];
      List when is_list(List) ->
          [to_binary(LI) || LI <- List];
      Bin when is_binary(Bin) ->
           [Bin];
      Other ->
           [to_binary(Other)]
    end.

-spec to_utf8_binary(Val) -> Result when
      Val :: unicode:latin1_chardata() | unicode:chardata() | unicode:external_chardata(),
      Result :: binary()
              | {error, binary(), RestData}
              | {incomplete, binary(), binary()},
      RestData :: unicode:latin1_chardata() | unicode:chardata() | unicode:external_chardata().
to_utf8_binary(Val) ->
    case unicode:characters_to_binary(Val, utf8) of
        {error, _, _} ->
            unicode:characters_to_binary(Val, latin1);
        UnicodeValue ->
            UnicodeValue
    end.

-spec to_unicode_charlist(Data) -> Result when
      Data :: unicode:latin1_chardata() | unicode:chardata() | unicode:external_chardata(),
      Result :: list()
              | {error, list(), RestData}
              | {incomplete, list(), binary()},
      RestData :: unicode:latin1_chardata() | unicode:chardata() | unicode:external_chardata().
to_unicode_charlist(Val) ->
    case unicode:characters_to_list(Val, utf8) of
        {error, _, _} ->
            unicode:characters_to_list(Val, latin1);
        UnicodeValue ->
            UnicodeValue
    end.

-spec as_list(list() | any()) -> [any()].
as_list(Nodes) when is_list(Nodes) ->
    Nodes;
as_list(Other) ->
    [Other].

-spec to_map_recursive(Val :: map() | list() | any()) -> map() | list() | any().
%% Recursively convert proplists to maps.
to_map_recursive([]) ->
    [];
to_map_recursive(Value) when is_list(Value) ->
    case is_proplist(Value) of
        true ->
            %% Normalize proplist by expanding bare atoms to {Atom, true}
            Normalized = normalize_proplist(Value),
            M = to_map(Normalized),
            maps:map(fun(_K, V) -> to_map_recursive(V) end, M);
        false ->
            %% The input is a regular list (e.g., federation-upstream-set)
            [to_map_recursive(V) || V <- Value]
    end;
to_map_recursive(Value) when is_map(Value) ->
    %% Recursively process map values
    maps:map(fun(_K, V) -> to_map_recursive(V) end, Value);
to_map_recursive(Value) ->
    %% Base case: atomic values (binary, integer, atom, etc.)
    Value.

-spec normalize_proplist(list()) -> [{atom() | binary() | list(), any()}].
normalize_proplist([]) ->
    [];
normalize_proplist([{K, V} | Rest]) ->
    [{K, V} | normalize_proplist(Rest)];
normalize_proplist([Atom | Rest]) when is_atom(Atom) ->
    [{Atom, true} | normalize_proplist(Rest)].

-spec is_proplist(any()) -> boolean().
is_proplist(List) when is_list(List) ->
    lists:all(fun is_proplist_element/1, List);
is_proplist(_) ->
    false.

-spec is_proplist_element(any()) -> boolean().
is_proplist_element({Key, _Value}) when is_atom(Key); is_binary(Key); is_list(Key) ->
    true;
is_proplist_element(Atom) when is_atom(Atom) ->
    true;
is_proplist_element(_) ->
    false.

list_to_boolean("true")  -> true;
list_to_boolean("false") -> false.
