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


-module(rabbit_auth_mechanism_ssl).

-behaviour(rabbit_auth_mechanism).

-export([description/0, should_offer/1, init/1, handle_response/2]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("public_key/include/public_key.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "auth mechanism external"},
                    {mfa,         {rabbit_registry, register,
                                   [auth_mechanism, <<"EXTERNAL">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready},
                    {cleanup,     {rabbit_registry, unregister,
                                   [auth_mechanism, <<"EXTERNAL">>]}}]}).

-record(state, {username = undefined}).

%% SASL EXTERNAL. SASL says EXTERNAL means "use credentials
%% established by means external to the mechanism". We define that to
%% mean the peer certificate's subject's DN or CN.

description() ->
    [{description, <<"SSL authentication mechanism using SASL EXTERNAL">>}].

should_offer(Sock) ->
    case rabbit_net:peercert(Sock) of
        nossl                -> false;
        {error, no_peercert} -> true; %% [0]
        {ok, _}              -> true
    end.
%% We offer EXTERNAL even if there is no peercert since that leads to
%% a more comprehensible error message - authentication is refused
%% below with "no peer certificate" rather than have the client fail
%% to negotiate an authentication mechanism.

init(Sock) ->
    Username = case rabbit_net:peercert(Sock) of
                   {ok, C} ->
                       case rabbit_ssl:peer_cert_auth_name(C) of
                           unsafe    -> {refused, none,
                                         "configuration unsafe", []};
                           not_found -> {refused, none, "no name found", []};
                           Name      -> Name
                       end;
                   {error, no_peercert} ->
                       {refused, none, "no peer certificate", []};
                   nossl ->
                       {refused, none, "not SSL connection", []}
               end,
    #state{username = Username}.

handle_response(_Response, #state{username = Username}) ->
    case Username of
        {refused, _, _, _} = E ->
            E;
        _ ->
            rabbit_access_control:check_user_login(Username, [])
    end.
