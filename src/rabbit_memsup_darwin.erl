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

-module(rabbit_memsup_darwin).

-export([init/0, update/2, get_memory_data/1]).

-record(state, {alarmed,
                total_memory,
                allocated_memory}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(state() :: #state { alarmed :: boolean(),
                          total_memory :: ('undefined' | non_neg_integer()),
                          allocated_memory :: ('undefined' | non_neg_integer())
                        }).

-spec(init/0 :: () -> state()).
-spec(update/2 :: (float(), state()) -> state()).
-spec(get_memory_data/1 :: (state()) -> {non_neg_integer(), non_neg_integer(),
                                         ('undefined' | pid())}).

-endif.

%%----------------------------------------------------------------------------

init() -> 
    #state{alarmed = false,
           total_memory = undefined,
           allocated_memory = undefined}.

update(MemoryFraction, State = #state{ alarmed = Alarmed }) ->
    File = os:cmd("/usr/bin/vm_stat"),
    Lines = string:tokens(File, "\n"),
    Dict = dict:from_list(lists:map(fun parse_line/1, Lines)),
    PageSize = dict:fetch(page_size, Dict),
    Inactive = dict:fetch('Pages inactive', Dict),
    Active = dict:fetch('Pages active', Dict),
    Free = dict:fetch('Pages free', Dict),
    Wired = dict:fetch('Pages wired down', Dict),
    MemTotal = PageSize * (Inactive + Active + Free + Wired),
    MemUsed = PageSize * (Active + Wired),
    NewAlarmed = MemUsed / MemTotal > MemoryFraction,
    case {Alarmed, NewAlarmed} of
        {false, true} ->
            alarm_handler:set_alarm({system_memory_high_watermark, []});
        {true, false} ->
            alarm_handler:clear_alarm(system_memory_high_watermark);
        _ ->
            ok
    end,
    State#state{alarmed = NewAlarmed,
                total_memory = MemTotal, allocated_memory = MemUsed}.

get_memory_data(State) ->
    {State#state.total_memory, State#state.allocated_memory, undefined}.

%%----------------------------------------------------------------------------

%% A line looks like "Foo bar: 123456."
parse_line(Line) ->
    [Name, RHS | _Rest] = string:tokens(Line, ":"),
    case Name of
        "Mach Virtual Memory Statistics" ->
            ["(page", "size", "of", PageSize, "bytes)"] =
                string:tokens(RHS, " "),
            {page_size, list_to_integer(PageSize)};
        _ ->
            [Value | _Rest1] = string:tokens(RHS, " ."),
            {list_to_atom(Name), list_to_integer(Value)}
    end.
