%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_data_coercion).

-export([to_binary/1, to_list/1, to_atom/1, to_integer/1, to_proplist/1, to_map/1]).
-export([to_atom/2, atomize_keys/1, to_list_of_binaries/1]).
-export([to_utf8_binary/1, to_unicode_charlist/1]).

-spec to_binary(Val :: binary() | list() | atom() | integer() | function()) -> binary().
to_binary(Val) when is_list(Val)     -> list_to_binary(Val);
to_binary(Val) when is_atom(Val)     -> atom_to_binary(Val, utf8);
to_binary(Val) when is_integer(Val)  -> integer_to_binary(Val);
to_binary(Val) when is_function(Val) -> list_to_binary(io_lib:format("~w", [Val]));
to_binary(Val)                       -> Val.

-spec to_list(Val :: integer() | list() | binary() | atom() | map()) -> list().
to_list(Val) when is_list(Val)    -> Val;
to_list(Val) when is_map(Val)     -> maps:to_list(Val);
to_list(Val) when is_atom(Val)    -> atom_to_list(Val);
to_list(Val) when is_binary(Val)  -> binary_to_list(Val);
to_list(Val) when is_integer(Val) -> integer_to_list(Val).

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
  maps:from_list(atomize_keys(maps:to_list(Val))).

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
