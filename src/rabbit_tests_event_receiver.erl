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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_tests_event_receiver).

-export([start/1, stop/0]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

start(Pid) ->
    gen_event:add_handler(rabbit_event, ?MODULE, [Pid]).

stop() ->
    gen_event:delete_handler(rabbit_event, ?MODULE, []).

%%----------------------------------------------------------------------------

init([Pid]) ->
    {ok, Pid}.

handle_call(_Request, Pid) ->
    {ok, not_understood, Pid}.

handle_event(Event, Pid) ->
    Pid ! Event,
    {ok, Pid}.

handle_info(_Info, Pid) ->
    {ok, Pid}.

terminate(_Arg, _Pid) ->
    ok.

code_change(_OldVsn, Pid, _Extra) ->
    {ok, Pid}.

%%----------------------------------------------------------------------------
