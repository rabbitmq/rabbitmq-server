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

-module(rabbit_connection_tracker).

%% Abstracts away how tracked connection records are stored
%% and queried.
%%
%% See also:
%%
%%  * rabbit_connection_tracking_handler
%%  * rabbit_reader
%%  * rabbit_event

-behaviour(gen_server2).

%% API
-export([boot/0, start_link/0, reregister/1]).

%% gen_fsm callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).


%%%===================================================================
%%% API
%%%===================================================================

boot() ->
  {ok, _} = start_link(),
  ok.

start_link() ->
  gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

reregister(Node) ->
  rabbit_log:info("Telling node ~p to re-register tracked connections", [Node]),
  gen_server2:cast({?SERVER, Node}, reregister).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
  {ok, {}}.

handle_call(_Req, _From, State) ->
  {noreply, State}.

handle_cast(reregister, State) ->
  Cs = rabbit_networking:connections_local(),
  rabbit_log:info("Connection tracker: asked to re-register ~p client connections", [length(Cs)]),
  case Cs of
    [] -> ok;
    Cs ->
      [reregister_connection(C) || C <- Cs],
      ok
  end,
  rabbit_log:info("Done re-registering client connections"),
  {noreply, State}.

handle_info(_Req, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

reregister_connection(Conn) ->
  try
    Conn ! reregister
  catch _:Error ->
    rabbit_log:error("Failed to re-register connection ~p after a network split: ~p", [Conn, Error])
  end.
