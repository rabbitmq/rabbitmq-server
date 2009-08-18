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

-module(rabbit_memsup_linux).

-export([init/0, update/1, get_memory_data/1]).

-record(state, {total_memory,
                allocated_memory}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(state() :: #state { total_memory :: ('undefined' | non_neg_integer()),
                          allocated_memory :: ('undefined' | non_neg_integer())
                        }).

-spec(init/0 :: () -> state()).
-spec(update/1 :: (state()) -> state()).
-spec(get_memory_data/1 :: (state()) -> {non_neg_integer(), non_neg_integer(),
                                         ('undefined' | pid())}).

-endif.

%%----------------------------------------------------------------------------

init() -> 
    #state{total_memory = undefined,
           allocated_memory = undefined}.

update(State) ->
    File = read_proc_file("/proc/meminfo"),
    Lines = string:tokens(File, "\n"),
    Dict = dict:from_list(lists:map(fun parse_line/1, Lines)),
    [MemTotal, MemFree, Buffers, Cached] =
        [dict:fetch(Key, Dict) ||
            Key <- ['MemTotal', 'MemFree', 'Buffers', 'Cached']],
    MemUsed = MemTotal - MemFree - Buffers - Cached,
    State#state{total_memory = MemTotal, allocated_memory = MemUsed}.

get_memory_data(State) ->
    {State#state.total_memory, State#state.allocated_memory, undefined}.

%%----------------------------------------------------------------------------

-define(BUFFER_SIZE, 1024).

%% file:read_file does not work on files in /proc as it seems to get
%% the size of the file first and then read that many bytes. But files
%% in /proc always have length 0, we just have to read until we get
%% eof.
read_proc_file(File) ->
    {ok, IoDevice} = file:open(File, [read, raw]),
    Res = read_proc_file(IoDevice, []),
    file:close(IoDevice),
    lists:flatten(lists:reverse(Res)).
    
read_proc_file(IoDevice, Acc) ->
    case file:read(IoDevice, ?BUFFER_SIZE) of
        {ok, Res} -> read_proc_file(IoDevice, [Res | Acc]);
        eof       -> Acc
    end.

%% A line looks like "FooBar: 123456 kB"
parse_line(Line) ->
    [Name, RHS | _Rest] = string:tokens(Line, ":"),
    [Value | UnitsRest] = string:tokens(RHS, " "),
    Value1 = case UnitsRest of
                 [] -> list_to_integer(Value); %% no units
                 ["kB"] -> list_to_integer(Value) * 1024
             end,
    {list_to_atom(Name), Value1}.
