%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

-module(rabbit_exchange_type_headers).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2, route/3]).
-export([validate/1, validate_binding/2,
         create/2, delete/2, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-export([info/1, info/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type headers"},
                    {mfa,         {rabbit_registry, register,
                                   [exchange, <<"headers">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{description, <<"AMQP headers exchange, as per the AMQP specification">>}].

serialise_events() -> false.

route(#exchange{name = Name}, Msg) ->
    route(#exchange{name = Name}, Msg, #{}).

route(#exchange{name = Name}, Msg, _Opts) ->
    %% TODO: find a way not to extract x-headers unless necessary
    Headers = mc:routing_headers(Msg, [x_headers]),

    rabbit_router:match_bindings(
      Name, fun(#binding{args = Args}) ->
                    case rabbit_misc:table_lookup(Args, <<"x-match">>) of
                        {longstr, <<"any">>} ->
                            match_any(Args, Headers, fun match/2);
                        {longstr, <<"any-with-x">>} ->
                            match_any(Args, Headers, fun match_x/2);
                        {longstr, <<"all-with-x">>} ->
                            match_all(Args, Headers, fun match_x/2);
                        _ ->
                            match_all(Args, Headers, fun match/2)
                    end
            end).

match_x({<<"x-match">>, _, _}, _M) ->
    skip;
match_x({K, void, _}, M) ->
    maps:is_key(K, M);
match_x({K, _, V}, M) ->
    maps:get(K, M, undefined) =:= V.

match({<<"x-", _/binary>>, _, _}, _M) ->
    skip;
match({K, void, _}, M) ->
    maps:is_key(K, M);
match({K, _, V}, M) ->
    maps:get(K, M, undefined) =:= V.


match_all([], _, _MatchFun) ->
    true;
match_all([Arg | Rem], M, Fun) ->
    case Fun(Arg, M) of
        false ->
            false;
        _ ->
            match_all(Rem, M, Fun)
    end.

match_any([], _, _Fun) ->
    false;
match_any([Arg | Rem], M, Fun) ->
    case Fun(Arg, M) of
        true ->
            true;
        _ ->
            match_any(Rem, M, Fun)
    end.

validate_binding(_X, #binding{args = Args}) ->
    case rabbit_misc:table_lookup(Args, <<"x-match">>) of
        {longstr, <<"all">>} -> ok;
        {longstr, <<"any">>} -> ok;
        {longstr, <<"all-with-x">>} -> ok;
        {longstr, <<"any-with-x">>} -> ok;
        {longstr, Other} ->
            {error, {binding_invalid,
                     "Invalid x-match field value ~tp; "
                     "expected all, any, all-with-x, or any-with-x", [Other]}};
        {Type, Other} ->
            {error, {binding_invalid,
                     "Invalid x-match field type ~tp (value ~tp); "
                     "expected longstr", [Type, Other]}};
        undefined -> ok %% [0]
    end.

validate(_X) -> ok.
create(_Serial, _X) -> ok.
delete(_Serial, _X) -> ok.
policy_changed(_X1, _X2) -> ok.
add_binding(_Serial, _X, _B) -> ok.
remove_bindings(_Serial, _X, _Bs) -> ok.
assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).
