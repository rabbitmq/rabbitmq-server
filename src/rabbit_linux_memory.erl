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

-module(rabbit_linux_memory).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(MEMORY_CHECK_INTERVAL, 1000).
-define(MEMORY_CHECK_FRACTION, 0.95).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


init(_Args) -> 
    {ok, no_alarm, ?MEMORY_CHECK_INTERVAL}.


handle_call(_Request, _From, State) -> 
    {noreply, State, ?MEMORY_CHECK_INTERVAL}.


handle_cast(_Request, State) -> 
    {noreply, State, ?MEMORY_CHECK_INTERVAL}.


handle_info(_Info, State) -> 
    File = read_proc_file("/proc/meminfo"),
    Lines = string:tokens(File, "\n"),
    Dict = dict:from_list(split_and_parse_lines(Lines, [])),
    MemTotal = dict:fetch("MemTotal", Dict),
    MemUsed = MemTotal 
            - dict:fetch("MemFree", Dict)
            - dict:fetch("Buffers", Dict)
            - dict:fetch("Cached", Dict),
    if 
        MemUsed / MemTotal > ?MEMORY_CHECK_FRACTION ->
            NewState = alarm;
        true ->
            NewState = no_alarm
    end,
    case {State, NewState} of
        {no_alarm, alarm} ->
            alarm_handler:set_alarm({system_memory_high_watermark, []}),
            ok;
        {alarm, no_alarm} ->
            alarm_handler:clear_alarm(system_memory_high_watermark),
            ok;
        _ ->
            ok
    end,
    {noreply, NewState, ?MEMORY_CHECK_INTERVAL}.

%% file:read_file does not work on files in /proc as it seems to get the size
%% of the file first and then read that many bytes. But files in /proc always
%% have length 0, we just have to read until we get eof.
read_proc_file(File) ->
    {ok, IoDevice} = file:open(File, [read, raw]),
    {ok, Res} = file:read(IoDevice, 1000000),
    Res.

%% A line looks like "FooBar: 123456 kB"
split_and_parse_lines([], Acc) -> Acc;
split_and_parse_lines([Line | Rest], Acc) ->
    Name = line_element(Line, 1),
    ValueString = line_element(Line, 2),
    Value = list_to_integer(string:sub_word(ValueString, 1)),
    split_and_parse_lines(Rest, [{Name, Value} | Acc]).

line_element(Line, Count) ->
    string:strip(string:sub_word(Line, Count, $:)).


terminate(_Reason, _State) -> 
    ok.


code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.
