%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_trust_store_app).
-behaviour(application).
-export([change_SSL_options/0]).
-export([revert_SSL_options/0]).
-export([start/2, stop/1]).

-rabbit_boot_step({rabbit_trust_store, [
    {description, "Overrides TLS options in take RabbitMQ trust store into account"},
    {mfa, {?MODULE, change_SSL_options, []}},
    {cleanup, {?MODULE, revert_SSL_options, []}},
    %% {requires, ...},
    {enables, networking}]}).

change_SSL_options() ->
    After = case application:get_env(rabbit, ssl_options) of
        undefined ->
            Before = [],
            edit(Before);
        {ok, Before} when is_list(Before) ->
            ok = application:set_env(rabbit, initial_SSL_options, Before),
            edit(Before)
    end,
    ok = application:set_env(rabbit,
        ssl_options, After).

revert_SSL_options() ->
    {ok, Cfg} = application:get_env(rabbit, initial_SSL_options),
    ok = application:set_env(rabbit, ssl_options, Cfg).

start(normal, _) ->
    rabbit_trust_store_sup:start_link().

stop(_) ->
    ok.


%% Ancillary & Constants

edit(Options) ->
    case proplists:get_value(verify_fun, Options) of
        undefined ->
            ok;
        Val       ->
            _ = rabbit_log:warning("RabbitMQ trust store plugin is used "
                               "and the verify_fun TLS option is set: ~p. "
                               "It will be overwritten by the plugin.~n", [Val]),
            ok
    end,
    %% Only enter those options neccessary for this application.
    lists:keymerge(1, required_options(),
        [{verify_fun, {delegate(), continue}},
         {partial_chain, fun partial_chain/1} | Options]).

delegate() -> fun rabbit_trust_store:whitelisted/3.

partial_chain(Chain) ->
    % special handling of clients that present a chain rather than just a peer cert.
    case lists:reverse(Chain) of
        [PeerDer, Ca | _] ->
            Peer = public_key:pkix_decode_cert(PeerDer, otp),
            % If the Peer is whitelisted make it's immediate Authority a trusted one.
            % This means the peer will automatically be validated.
            case rabbit_trust_store:is_whitelisted(Peer) of
                true -> {trusted_ca, Ca};
                false -> unknown_ca
            end;
        _ -> unknown_ca
    end.

required_options() ->
    [{verify, verify_peer}, {fail_if_no_peer_cert, true}].
