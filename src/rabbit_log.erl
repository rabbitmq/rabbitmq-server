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

-module(rabbit_log).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([debug/1, debug/2, message/4, info/1, info/2,
         warning/1, warning/2, error/1, error/2]).

-import(io).
-import(error_logger).

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> {'ok', pid()} | 'ignore' | {'error', any()}).
-spec(debug/1 :: (string()) -> 'ok').
-spec(debug/2 :: (string(), [any()]) -> 'ok').
-spec(info/1 :: (string()) -> 'ok').
-spec(info/2 :: (string(), [any()]) -> 'ok').
-spec(warning/1 :: (string()) -> 'ok').
-spec(warning/2 :: (string(), [any()]) -> 'ok').
-spec(error/1 :: (string()) -> 'ok').
-spec(error/2 :: (string(), [any()]) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

debug(Fmt) ->
    gen_server:cast(?SERVER, {debug, Fmt}).

debug(Fmt, Args) when is_list(Args) ->
    gen_server:cast(?SERVER, {debug, Fmt, Args}).

message(Direction, Channel, MethodRecord, Content) ->
    gen_server:cast(?SERVER,
                    {message, Direction, Channel, MethodRecord, Content}).

info(Fmt) ->
    gen_server:cast(?SERVER, {info, Fmt}).

info(Fmt, Args) when is_list(Args) ->
    gen_server:cast(?SERVER, {info, Fmt, Args}).

warning(Fmt) ->
    gen_server:cast(?SERVER, {warning, Fmt}).

warning(Fmt, Args) when is_list(Args) ->
    gen_server:cast(?SERVER, {warning, Fmt, Args}).

error(Fmt) ->
    gen_server:cast(?SERVER, {error, Fmt}).

error(Fmt, Args) when is_list(Args) ->
    gen_server:cast(?SERVER, {error, Fmt, Args}).

%%--------------------------------------------------------------------

init([]) -> {ok, none}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({debug, Fmt}, State) ->
    io:format("debug:: "), io:format(Fmt),
    error_logger:info_msg("debug:: " ++ Fmt),
    {noreply, State};
handle_cast({debug, Fmt, Args}, State) ->
    io:format("debug:: "), io:format(Fmt, Args),
    error_logger:info_msg("debug:: " ++ Fmt, Args),
    {noreply, State};
handle_cast({message, Direction, Channel, MethodRecord, Content}, State) ->
    io:format("~s ch~p ~p~n",
              [case Direction of
                   in -> "-->";
                   out -> "<--" end,
               Channel,
               {MethodRecord, Content}]),
    {noreply, State};
handle_cast({info, Fmt}, State) ->
    error_logger:info_msg(Fmt),
    {noreply, State};
handle_cast({info, Fmt, Args}, State) ->
    error_logger:info_msg(Fmt, Args),
    {noreply, State};
handle_cast({warning, Fmt}, State) ->
    error_logger:warning_msg(Fmt),
    {noreply, State};
handle_cast({warning, Fmt, Args}, State) ->
    error_logger:warning_msg(Fmt, Args),
    {noreply, State};
handle_cast({error, Fmt}, State) ->
    error_logger:error_msg(Fmt),
    {noreply, State};
handle_cast({error, Fmt, Args}, State) ->
    error_logger:error_msg(Fmt, Args),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

