%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_prop_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(rabbit_data_coercion, [to_map_recursive/1]).

-compile(export_all).

all() ->
    [
        {group, parallel_tests}
    ].

groups() ->
    [
        {parallel_tests, [parallel], [
            data_coercion_as_list_property,
            data_coercion_to_binary_roundtrip_property,
            data_coercion_to_map_recursive_property
        ]}
    ].

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.
init_per_testcase(_, Config) -> Config.
end_per_testcase(_, Config) -> Config.

proper_opts() ->
    [{numtests, 100},
     {on_output, fun(".", _) -> ok;
                    (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                 end}].

data_coercion_as_list_property(_Config) ->
    ?assert(
       proper:quickcheck(
         ?FORALL(Val, oneof([list(integer()), binary(), atom(), integer(),
                             {atom(), integer()}, #{atom() => integer()}]),
                 begin
                     Result = rabbit_data_coercion:as_list(Val),
                     %% Result is always a list
                     is_list(Result) andalso
                     %% Lists pass through, non-lists get wrapped
                     case is_list(Val) of
                         true -> Result =:= Val;
                         false -> Result =:= [Val]
                     end
                 end),
         proper_opts())).

data_coercion_to_binary_roundtrip_property(_Config) ->
    %% Verify roundtrip: value -> binary -> value
    ?assert(
       proper:quickcheck(
         ?FORALL({Type, Val}, oneof([
                     {binary, binary()},
                     {integer, integer()},
                     {atom, oneof([foo, bar, baz, true, false, hello, world])}
                 ]),
                 begin
                     Binary = rabbit_data_coercion:to_binary(Val),
                     is_binary(Binary) andalso
                     case Type of
                         binary -> Binary =:= Val;
                         integer -> rabbit_data_coercion:to_integer(Binary) =:= Val;
                         atom -> rabbit_data_coercion:to_atom(Binary) =:= Val
                     end
                 end),
         proper_opts())).

data_coercion_to_map_recursive_property(_Config) ->
    ?assert(
       proper:quickcheck(
         ?FORALL(Input, proplist_or_value(),
                 begin
                     Result = to_map_recursive(Input),
                     to_map_recursive(Result) =:= Result
                 end),
         [{numtests, 100},
          {max_size, 10},
          {on_output, fun(".", _) -> ok;
                         (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                      end}])).

proplist_or_value() ->
    ?LAZY(oneof([
        oneof([key1, key2, key3]),
        binary(),
        integer(),
        ?SIZED(Size, resize(Size div 2, proplist_gen())),
        ?SIZED(Size, resize(Size div 2, list(proplist_or_value())))
    ])).

proplist_key() ->
    oneof([
        oneof([key1, key2, key3]),
        binary(),
        ?LET(S, list(oneof([integer(97, 122)])), S)
    ]).

proplist_gen() ->
    list(oneof([
        {proplist_key(), proplist_or_value()},
        oneof([enabled, disabled, auto])
    ])).
