%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_mechanism_plain).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_auth_mechanism).

-export([description/0, should_offer/1, init/1, handle_response/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "auth mechanism plain"},
                    {mfa,         {rabbit_registry, register,
                                   [auth_mechanism, <<"PLAIN">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

%% SASL PLAIN, as used by the Qpid Java client and our clients. Also,
%% apparently, by OpenAMQ.

description() ->
    [{description, <<"SASL PLAIN authentication mechanism">>}].

should_offer(_Sock) ->
    true.

init(_Sock) ->
    [].

handle_response(Response, _State) ->
    case extract_user_pass(Response) of
        {ok, User, Pass} ->
            rabbit_access_control:check_user_pass_login(User, Pass);
        error ->
            {protocol_error, "response ~p invalid", [Response]}
    end.

extract_user_pass(Response) ->
    case extract_elem(Response) of
        {ok, User, Response1} -> case extract_elem(Response1) of
                                     {ok, Pass, <<>>} -> {ok, User, Pass};
                                     _                -> error
                                 end;
        error                 -> error
    end.

extract_elem(<<0:8, Rest/binary>>) ->
    Count = next_null_pos(Rest, 0),
    <<Elem:Count/binary, Rest1/binary>> = Rest,
    {ok, Elem, Rest1};
extract_elem(_) ->
    error.

next_null_pos(<<>>, Count)                  -> Count;
next_null_pos(<<0:8, _Rest/binary>>, Count) -> Count;
next_null_pos(<<_:8, Rest/binary>>,  Count) -> next_null_pos(Rest, Count + 1).
