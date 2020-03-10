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

-module(dummy_event_receiver).

-export([start/3, stop/0]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-include("rabbit.hrl").

start(Pid, Nodes, Types) ->
    Oks = [ok || _ <- Nodes],
    {Oks, _} = rpc:multicall(Nodes, gen_event, add_handler,
                             [rabbit_event, ?MODULE, [Pid, Types]]).

stop() ->
    gen_event:delete_handler(rabbit_event, ?MODULE, []).

%%----------------------------------------------------------------------------

init([Pid, Types]) ->
    {ok, {Pid, Types}}.

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event(Event = #event{type = Type}, State = {Pid, Types}) ->
    case lists:member(Type, Types) of
        true  -> Pid ! Event;
        false -> ok
    end,
    {ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
