%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_alarm).

-behaviour(gen_event).

-export([start/0, stop/0]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-define(MEMSUP_CHECK_INTERVAL, 1000).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> 'ok').
-spec(stop/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start() ->
    %% The default memsup check interval is 1 minute, which is way too
    %% long - rabbit can gobble up all memory in a matter of
    %% seconds. Unfortunately the memory_check_interval configuration
    %% parameter and memsup:set_check_interval/1 function only provide
    %% a granularity of minutes. So we have to peel off one layer of
    %% the API to get to the underlying layer which operates at the
    %% granularity of milliseconds.
    ok = os_mon:call(memsup, {set_check_interval, ?MEMSUP_CHECK_INTERVAL},
                     infinity),

    ok = alarm_handler:add_alarm_handler(?MODULE).

stop() ->
    ok = alarm_handler:delete_alarm_handler(?MODULE).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, none}.

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event(Event, State) ->
    {ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
