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

-module(rabbit_log).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([info/1, info/2, warning/1, warning/2, error/1, error/2]).

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
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

