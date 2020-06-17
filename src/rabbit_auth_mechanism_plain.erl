%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_mechanism_plain).
-include("rabbit.hrl").

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
