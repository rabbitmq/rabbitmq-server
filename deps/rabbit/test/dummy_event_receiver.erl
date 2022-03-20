%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(dummy_event_receiver).

-export([start/3, stop/0]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("rabbit_common/include/rabbit.hrl").

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
