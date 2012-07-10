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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_alarm_handler).

-behaviour(gen_event).

-export([start/0]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

start() ->
    alarm_handler:delete_alarm_handler(alarm_handler),
    alarm_handler:add_alarm_handler(?MODULE).

init(_) ->
    {ok, []}.

handle_event({set_alarm, Alarm}, Alarms)->
    error_logger:info_report([{alarm_handler, {set, Alarm}}]),
    {ok, [Alarm | Alarms]};
handle_event({clear_alarm, AlarmId}, Alarms)->
    error_logger:info_report([{alarm_handler, {clear, AlarmId}}]),
    {ok, lists:keydelete(AlarmId, 1, Alarms)};
handle_event(_, Alarms)->
    {ok, Alarms}.

handle_info(_, Alarms) -> {ok, Alarms}.

handle_call(get_alarms, Alarms) -> {ok, Alarms, Alarms};
handle_call(_Query, Alarms)     -> {ok, {error, bad_query}, Alarms}.

terminate(swap, Alarms) ->
    {alarm_handler, Alarms};
terminate(_, _) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
