%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(mirrored_supervisor_SUITE_gs).

%% Dumb gen_server we can supervise

-export([start_link/1]).

-export([init/1, handle_call/3, handle_info/2, terminate/2, code_change/3,
         handle_cast/2]).

-behaviour(gen_server).

-define(MS,  mirrored_supervisor).

start_link(want_error) ->
    {error, foo};

start_link(want_exit) ->
    exit(foo);

start_link(Id) ->
    gen_server:start_link({local, Id}, ?MODULE, [], []).

%% ---------------------------------------------------------------------------

init([]) ->
    {ok, state}.

handle_call(Msg, _From, State) ->
    die_if_my_supervisor_is_evil(),
    {reply, {received, self(), Msg}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

die_if_my_supervisor_is_evil() ->
    try lists:keysearch(self(), 2, ?MS:which_children(evil)) of
        false -> ok;
        _     -> exit(doooom)
    catch
        exit:{noproc, _} -> ok
    end.
