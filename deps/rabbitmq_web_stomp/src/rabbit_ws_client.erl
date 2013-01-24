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
%% Copyright (c) 2012-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_ws_client).
-behaviour(gen_server).

-export([start_link/1]).
-export([sockjs_msg/2, sockjs_closed/1]).

-export([init/1, handle_call/3, handle_info/2, terminate/2,
         code_change/3, handle_cast/2]).

-record(state, {conn, processor, parse_state}).

%%----------------------------------------------------------------------------

start_link(Params) ->
    gen_server:start_link(?MODULE, Params, []).

sockjs_msg(Pid, Data) ->
    gen_server:cast(Pid, {sockjs_msg, Data}).

sockjs_closed(Pid) ->
    gen_server:cast(Pid, sockjs_closed).

%%----------------------------------------------------------------------------

init({Processor, Conn}) ->
    ok = file_handle_cache:obtain(),
    process_flag(trap_exit, true),
    {ok, #state{conn        = Conn,
                processor   = Processor,
                parse_state = rabbit_stomp_frame:initial_state()}}.

handle_cast({sockjs_msg, Data}, State = #state{processor   = Processor,
                                               parse_state = ParseState}) ->
    ParseState1 = process_received_bytes(Data, Processor, ParseState),
    {noreply, State#state{parse_state = ParseState1}};

handle_cast(sockjs_closed, State) ->
    {stop, normal, State};

handle_cast(Cast, State) ->
    {stop, {odd_cast, Cast}, State}.

%% TODO this is a bit rubbish - after the preview release we should
%% make the credit_flow:send/1 invocation in
%% rabbit_stomp_processor:process_frame/2 optional.
handle_info({bump_credit, {_, _}}, State) ->
    {noreply, State};

handle_info(Info, State) ->
    {stop, {odd_info, Info}, State}.

handle_call(Request, _From, State) ->
    {stop, {odd_request, Request}, State}.

terminate(Reason, #state{conn = Conn, processor = Processor}) ->
    ok = file_handle_cache:release(),
    _ = case Reason of
            normal -> % SockJS initiated exit
                rabbit_stomp_processor:flush_and_die(Processor);
            shutdown -> % STOMP died
                Conn:close(1000, "STOMP died")
        end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%----------------------------------------------------------------------------

process_received_bytes(Bytes, Processor, ParseState) ->
    case rabbit_stomp_frame:parse(Bytes, ParseState) of
        {ok, Frame, Rest} ->
            rabbit_stomp_processor:process_frame(Processor, Frame),
            ParseState1 = rabbit_stomp_frame:initial_state(),
            process_received_bytes(Rest, Processor, ParseState1);
        {more, ParseState1} ->
            ParseState1
    end.


