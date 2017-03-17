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
%% Copyright (c) 2017 Pivotal Software, Inc.  All rights reserved.
%%

%% This module implements a watcher process which should stop
%% the parent supervisor if its vhost is missing from the mnesia DB

-module(rabbit_vhost_sup_watcher).

-include("rabbit.hrl").

-define(TICKTIME_RATIO, 4).

-behaviour(gen_server2).
-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).


start_link(VHost) ->
    gen_server2:start_link(?MODULE, [VHost], []).


init([VHost]) ->
    Interval = interval(),
    timer:send_interval(Interval, check_vhost),
    {ok, VHost}.

handle_call(_,_,VHost) ->
    {reply, ok, VHost}.

handle_cast(_, VHost) ->
    {noreply, VHost}.

handle_info(check_vhost, VHost) ->
    case rabbit_vhost:exists(VHost) of
        true  -> {noreply, VHost};
        false ->
            rabbit_log:error(" Vhost \"~p\" is gone."
                             " Stopping message store supervisor.",
                             [VHost]),
            {stop, normal, VHost}
    end;
handle_info(_, VHost) ->
    {noreply, VHost}.

terminate(_, _) -> ok.

code_change(_OldVsn, VHost, _Extra) ->
    {ok, VHost}.

interval() ->
    application:get_env(kernel, net_ticktime, 60000) * ?TICKTIME_RATIO.