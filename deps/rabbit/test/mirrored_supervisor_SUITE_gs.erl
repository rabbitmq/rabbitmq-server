%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
