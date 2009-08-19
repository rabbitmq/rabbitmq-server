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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_memsup).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([update/0]).

-record(state, {memory_fraction,
                timeout,
                timer,
                mod,
                mod_state,
                alarmed
               }).

-define(SERVER, memsup). %% must be the same as the standard memsup

-define(DEFAULT_MEMORY_CHECK_INTERVAL, 1000).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/1 :: (atom()) -> {'ok', pid()} | 'ignore' | {'error', any()}).
-spec(update/0 :: () -> 'ok').
     
-endif.

%%----------------------------------------------------------------------------

start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Args], []).

update() ->
    gen_server:cast(?SERVER, update).

%%----------------------------------------------------------------------------

init([Mod]) -> 
    Fraction = os_mon:get_env(memsup, system_memory_high_watermark),
    TRef = start_timer(?DEFAULT_MEMORY_CHECK_INTERVAL),
    InitState = Mod:init(),
    State = #state { memory_fraction = Fraction,
                     timeout = ?DEFAULT_MEMORY_CHECK_INTERVAL,
                     timer = TRef,
                     mod = Mod,
                     mod_state = InitState,
                     alarmed = false },
    {ok, internal_update(State)}.

start_timer(Timeout) ->
    {ok, TRef} = timer:apply_interval(Timeout, ?MODULE, update, []),
    TRef.

%% Export the same API as the real memsup. Note that
%% get_sysmem_high_watermark gives an int in the range 0 - 100, while
%% set_sysmem_high_watermark takes a float in the range 0.0 - 1.0.
handle_call(get_sysmem_high_watermark, _From, State) ->
    {reply, trunc(100 * State#state.memory_fraction), State};

handle_call({set_sysmem_high_watermark, Float}, _From, State) ->
    {reply, ok, State#state{memory_fraction = Float}};

handle_call(get_check_interval, _From, State) ->
    {reply, State#state.timeout, State};

handle_call({set_check_interval, Timeout}, _From, State) ->
    {ok, cancel} = timer:cancel(State#state.timer),
    {reply, ok, State#state{timeout = Timeout, timer = start_timer(Timeout)}};

handle_call(get_memory_data, _From,
            State = #state { mod = Mod, mod_state = ModState }) ->
    {reply, Mod:get_memory_data(ModState), State};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(update, State) ->
    {noreply, internal_update(State)};

handle_cast(_Request, State) -> 
    {noreply, State}.

handle_info(_Info, State) -> 
    {noreply, State}.

terminate(_Reason, _State) -> 
    ok.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.
 
internal_update(State = #state { memory_fraction = MemoryFraction,
                                 alarmed = Alarmed,
                                 mod = Mod, mod_state = ModState }) ->
    ModState1 = Mod:update(ModState),
    {MemTotal, MemUsed, _BigProc} = Mod:get_memory_data(ModState1),
    NewAlarmed = MemUsed / MemTotal > MemoryFraction,
    case {Alarmed, NewAlarmed} of
        {false, true} ->
            alarm_handler:set_alarm({system_memory_high_watermark, []});
        {true, false} ->
            alarm_handler:clear_alarm(system_memory_high_watermark);
        _ ->
            ok
    end,    
    State #state { mod_state = ModState1, alarmed = NewAlarmed }.
