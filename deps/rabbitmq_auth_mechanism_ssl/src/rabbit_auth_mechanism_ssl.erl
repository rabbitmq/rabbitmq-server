%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%


-module(rabbit_auth_mechanism_ssl).

-behaviour(rabbit_auth_mechanism).

-export([description/0, should_offer/1, init/1, handle_response/2]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("public_key/include/public_key.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "external TLS peer verification-based authentication mechanism"},
                    {mfa,         {rabbit_registry, register,
                                   [auth_mechanism, <<"EXTERNAL">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready},
                    {cleanup,     {rabbit_registry, unregister,
                                   [auth_mechanism, <<"EXTERNAL">>]}}]}).

-record(state, {username = undefined}).

description() ->
    [{description, <<"TLS peer verification-based authentication plugin. Used in combination with the EXTERNAL SASL mechanism.">>}].

should_offer(Sock) ->
    %% SASL EXTERNAL. SASL says EXTERNAL means "use credentials
    %% established by means external to the mechanism". The username
    %% is extracted from the the client certificate.
    case rabbit_net:peercert(Sock) of
        nossl                -> false;
        %% We offer EXTERNAL even if there is no peercert since that leads to
        %% a more comprehensible error message: authentication is refused
        %% below with "no peer certificate" rather than have the client fail
        %% to negotiate an authentication mechanism.
        {error, no_peercert} -> true;
        {ok, _}              -> true
    end.

init(Sock) ->
    Username = case rabbit_net:peercert(Sock) of
                   {ok, C} ->
                       case rabbit_ssl:peer_cert_auth_name(C) of
                           unsafe    -> {refused, none, "TLS configuration is unsafe", []};
                           not_found -> {refused, none, "no name found", []};
                           Name      ->
                               %% strip any leading and trailing newlines, we assume that they are never
                               %% added intentionally. For SANS of type otherName, the Erlang ASN.1/public key parser
                               %% seems to add some in certain cases
                               Val = rabbit_data_coercion:to_binary(Name),
                               rabbit_log:debug("auth mechanism TLS extracted username '~s' from peer certificate", [Val]),
                               Val
                       end;
                   {error, no_peercert} ->
                       {refused, none, "connection peer presented no TLS (x.509) certificate", []};
                   nossl ->
                       {refused, none, "not a TLS-enabled connection", []}
               end,
    #state{username = Username}.

handle_response(_Response, #state{username = Username}) ->
    case Username of
        {refused, _, _, _} = E ->
            E;
        _ ->
            rabbit_access_control:check_user_login(Username, [])
    end.
