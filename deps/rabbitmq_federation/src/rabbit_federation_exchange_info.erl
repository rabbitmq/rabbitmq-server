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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_exchange_info).

-behaviour(gen_server2).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_federation.hrl").

-export([start_link/1]).

-export([args/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(Args) ->
    gen_server2:start_link(?MODULE, Args, [{timeout, infinity}]).

args(Pid) ->
    gen_server2:call(Pid, args, infinity).

%%----------------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

handle_call(args, _From, Args) ->
    {reply, Args, Args};

handle_call(_Msg, _From, Args) ->
    {reply, ok, Args}.

handle_cast(_Msg, Args) ->
    {noreply, Args}.

handle_info(_Msg, Args) ->
    {noreply, Args}.

code_change(_OldVsn, Args, _Extra) ->
    {ok, Args}.

terminate(_Reason, {_URIs, DownstreamX, _Durable}) ->
    true = ets:delete(?ETS_NAME, DownstreamX),
    ok.
