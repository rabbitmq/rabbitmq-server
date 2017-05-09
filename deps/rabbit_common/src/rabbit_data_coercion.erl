%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_data_coercion).

-export([to_binary/1, to_list/1, to_atom/1, to_integer/1]).
-export([to_atom/2]).

-spec to_binary(Val :: binary() | list() | atom() | integer()) -> binary().
to_binary(Val) when is_list(Val)    -> list_to_binary(Val);
to_binary(Val) when is_atom(Val)    -> atom_to_binary(Val, utf8);
to_binary(Val) when is_integer(Val) -> integer_to_binary(Val);
to_binary(Val)                      -> Val.

-spec to_list(Val :: integer() | list() | binary() | atom()) -> list().
to_list(Val) when is_list(Val)    -> Val;
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
