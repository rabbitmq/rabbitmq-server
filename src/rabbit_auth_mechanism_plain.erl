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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_auth_mechanism_plain).
-include("rabbit.hrl").

-behaviour(rabbit_auth_mechanism).

-export([description/0, init/1, handle_response/2]).

-include("rabbit_auth_mechanism_spec.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "auth mechanism plain"},
                    {mfa,         {rabbit_registry, register,
                                   [auth_mechanism, <<"PLAIN">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

%% SASL PLAIN, as used by the Qpid Java client and our clients. Also,
%% apparently, by OpenAMQ.

description() ->
    [{name, <<"PLAIN">>},
     {description, <<"SASL PLAIN authentication mechanism">>}].

init(_Sock) ->
    [].

handle_response(Response, _State) ->
    {User, Response1} = split_on_null(drop_leading_null(Response), []),
    {Pass, _Response2} = split_on_null(Response1, []),
    rabbit_access_control:check_user_pass_login(
      list_to_binary(User), list_to_binary(Pass)).

drop_leading_null(<<0:8, Rest/binary>>) ->
    Rest.

split_on_null(<<0:8, Rest/binary>>, Acc) ->
    {lists:reverse(Acc), Rest};
split_on_null(<<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
split_on_null(<<C:8, Rest/binary>>, Acc) ->
    split_on_null(Rest, [C | Acc]).
