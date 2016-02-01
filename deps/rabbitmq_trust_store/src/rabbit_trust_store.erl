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
-export([start/1, start_link/1,
         stop/1]).
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

stop(Id) ->
    gen_server:call(Id, stop).


%% API

-spec whitelisted(certificate(), event(), state()) -> outcome().
whitelisted(_, valid_peer, _) ->
    %% A certificate, in a chain, considered valid.
    rabbit_log:error("whitelisted/3 clause ~p, valid certificate in chain.~n", [0]),
    {valid, []};
whitelisted(_, valid, _) ->
    %% A trusted root certificate (from an authority).
    rabbit_log:error("whitelisted/3 clause ~p, valid root certificate.~n", [1]),
    {valid, []};
whitelisted(_, {bad_cert, selfsigned_peer}, _) ->
    %% May be whitelisted.
    rabbit_log:error("whitelisted/3 clause ~p, no certificate chain as such.~n", [2]),
    {fail, "could go either way."}; %% Could succeed or fail!
whitelisted(_, {bad_cert, _} = Reason, _) ->
    %% Any other reason we can fail.
    rabbit_log:error("whitelisted/3 clause ~p, reason ~p.~n", [3, Reason]),
    {fail, Reason};
whitelisted(_, {extension, _}, _) ->
    %% We don't handle any extensions, though future functionality may rely on this.
    rabbit_log:error("whitelisted/3 clause ~p.~n", [4]),
    {unknown, []}.


%% Generic Server Callback

init({whitelist, _Path}) ->
    {ok, {}}.

handle_call(stop, _, St) ->
    {stop, normal, ok, St}.

handle_cast(stop, St) ->
    {stop, normal, St}. %% OTP 18: Generic Server machinery will call `terminate/2'.

handle_info(_, St) ->
    {noreply, St}.

terminate(_, _St) ->
    ok.

code_change(_,_,_) ->
    {error, no}.
