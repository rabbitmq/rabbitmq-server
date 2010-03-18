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
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_alarm).

-behaviour(gen_event).

-export([start/0, stop/0, register/2]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-record(alarms, {alertees, vm_memory_high_watermark = false}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(mfa_tuple() :: {atom(), atom(), list()}).
-spec(start/0 :: () -> 'ok').
-spec(stop/0 :: () -> 'ok').
-spec(register/2 :: (pid(), mfa_tuple()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start() ->
    ok = alarm_handler:add_alarm_handler(?MODULE, []),
    {ok, MemoryWatermark} = application:get_env(vm_memory_high_watermark),
    ok = case MemoryWatermark == 0 of
             true  -> ok;
             false -> rabbit_sup:start_restartable_child(vm_memory_monitor,
                                                         [MemoryWatermark])
         end,
    ok.

stop() ->
    ok = alarm_handler:delete_alarm_handler(?MODULE).

register(Pid, HighMemMFA) ->
    ok = gen_event:call(alarm_handler, ?MODULE,
                        {register, Pid, HighMemMFA},
                        infinity).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #alarms{alertees = dict:new()}}.

handle_call({register, Pid, {M, F, A} = HighMemMFA},
            State = #alarms{alertees = Alertess}) ->
    _MRef = erlang:monitor(process, Pid),
    ok = case State#alarms.vm_memory_high_watermark of
             true  -> apply(M, F, A ++ [Pid, true]);
             false -> ok
         end,
    NewAlertees = dict:store(Pid, HighMemMFA, Alertess),
    {ok, ok, State#alarms{alertees = NewAlertees}};

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event({set_alarm, {vm_memory_high_watermark, []}}, State) ->
    ok = alert(true, State#alarms.alertees),
    {ok, State#alarms{vm_memory_high_watermark = true}};

handle_event({clear_alarm, vm_memory_high_watermark}, State) ->
    ok = alert(false, State#alarms.alertees),
    {ok, State#alarms{vm_memory_high_watermark = false}};

handle_event(_Event, State) ->
    {ok, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #alarms{alertees = Alertess}) ->
    {ok, State#alarms{alertees = dict:erase(Pid, Alertess)}};

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
alert(_Alert, undefined) ->
    ok;
alert(Alert, Alertees) ->
    dict:fold(fun (Pid, {M, F, A}, Acc) ->
                      ok = erlang:apply(M, F, A ++ [Pid, Alert]),
                      Acc
              end, ok, Alertees).
