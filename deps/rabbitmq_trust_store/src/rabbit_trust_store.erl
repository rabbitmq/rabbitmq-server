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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_trust_store).
-export([whitelisted/3]).
-export([start/1, start_link/1]).
-behaviour(gen_server).
-export([init/1, terminate/2,
         handle_call/3, handle_cast/2,
         handle_info/2,
         code_change/3]).

-include_lib("public_key/include/public_key.hrl").
-type certificate() :: #'OTPCertificate'{}.
-type event()       :: valid_peer
                     | valid
                     | {bad_cert, Other :: atom()
                                | unknown_ca
                                | selfsigned_peer}
                     | {extension, #'Extension'{}}.
-type state()       :: term().
-type outcome()     :: {valid, state()}
                     | {fail, Reason :: term()}
                     | {unknown, state()}.

%% OTP Supervision

start({whitelist, Path}) ->
    gen_server:start(?MODULE, {whitelist, Path}, []).

start_link({whitelist, Path}) ->
    gen_server:start_link({local, trust_store}, ?MODULE, {whitelist, Path}, []).


%% API

-spec whitelisted(certificate(), event(), state()) -> outcome().
whitelisted(_, {bad_cert, unknown_ca}, St) ->
    %% This is in fact ONE of the base cases: once path validation has
    %% reached the final certificate in a chain.
    rabbit_log:error("~p: whitelisted/3 clause `unknown CA`.~n", [St]),
    {fail, "Not a trusted CA."}; %% Could succeed or fail!
whitelisted(_, {bad_cert, selfsigned_peer}, St) ->
    %% ANOTHER base case: may be whitelisted.
    rabbit_log:error("~p: whitelisted/3 clause `self-signed peer`: no certificate *chain* as such.~n", [St]),
    {fail, "Self-signed peer."}; %% Could succeed or fail!
whitelisted(_, valid, St) ->
    %% A trusted root certificate (from an authority).
    rabbit_log:error("~p: whitelisted/3 clause `valid chain`: valid root certificate.~n", [St]),
    {valid, St};
whitelisted(_, valid_peer, St) ->
    %% A certificate, in a chain, considered valid up to this point.
    rabbit_log:error("~p: whitelisted/3 clause `valid peer`: valid certificate in chain.~n", [St]),
    {valid, [random:uniform()]};
whitelisted(_, {bad_cert, _} = Reason, St) ->
    %% Any other reason we can fail.
    rabbit_log:error("~p: whitelisted/3 clause `bad certificate` (catch-all): reason ~p.~n", [St, Reason]),
    {fail, Reason};
whitelisted(_, {extension, _}, St) ->
    %% We don't handle any extensions, though future functionality may rely on this.
    rabbit_log:error("whitelisted/3 clause `extension`.~n", []),
    {unknown, []}.


%% Generic Server Callback

init({whitelist, _Path}) ->
    _ = erlang:process_flag(trap_exit, true),
    {ok, {}}.

handle_call(_, _, St) ->
    {reply, ok, St}.

handle_cast(_, St) ->
    {noreply, St}.

handle_info(_, St) ->
    {noreply, St}.

terminate(shutdown, _St) ->
    ok.

code_change(_,_,_) ->
    {error, no}.
