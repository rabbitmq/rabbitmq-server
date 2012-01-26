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

-export([message/4, log/3, log/4,
         debug/1, debug/2, info/1, info/2,
         warning/1, warning/2, error/1, error/2]).

-define(SERVER, ?MODULE).

-define(LEVELS, [debug, info, warning, error, none]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([level/0]).

-type(category() :: atom()).
-type(level() :: 'debug' | 'info' | 'warning' | 'error').

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).

-spec(message/4 :: (_,_,_,_) -> 'ok').
-spec(log/3 :: (category(), level(), string()) -> 'ok').
-spec(log/4 :: (category(), level(), string(), [any()]) -> 'ok').
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

-record(state, {levels, config}).

%%----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [?LEVELS], []).

message(Direction, Channel, MethodRecord, Content) ->
    gen_server:cast(?SERVER,
                    {message, Direction, Channel, MethodRecord, Content}).

log(Category, Level, Fmt) -> log(Category, Level, Fmt, []).

log(Category, Level, Fmt, Args) when is_list(Args) ->
    gen_server:cast(?SERVER, {log, Category, Level, Fmt, Args}).

debug(Fmt)         -> log(default, debug,   Fmt).
debug(Fmt, Args)   -> log(default, debug,   Fmt, Args).
info(Fmt)          -> log(default, info,    Fmt).
info(Fmt, Args)    -> log(default, info,    Fmt, Args).
warning(Fmt)       -> log(default, warning, Fmt).
warning(Fmt, Args) -> log(default, warning, Fmt, Args).
error(Fmt)         -> log(default, error,   Fmt).
error(Fmt, Args)   -> log(default, error,   Fmt, Args).

%%--------------------------------------------------------------------

init([Levels]) ->
    {ok, LevelConfig} = application:get_env(log_levels),
    {ok, #state{levels = orddict:from_list(
                           lists:zip(Levels, lists:seq(1, length(Levels)))),
                config = orddict:from_list(LevelConfig)}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({log, Category, Level, Fmt, Args},
            State = #state{levels = Levels, config = Config}) ->
    CatLevel = case orddict:find(Category, Config) of
                   {ok, L} -> L;
                   error   -> info
               end,
    case orddict:fetch(Level, Levels) >= orddict:fetch(CatLevel, Levels) of
        false -> ok;
        true  -> case Level of
                     debug   -> io:format("debug:: " ++ Fmt, Args),
                                error_logger:info_msg("debug:: " ++ Fmt, Args);
                     info    -> error_logger:info_msg(Fmt, Args);
                     warning -> error_logger:warning_msg(Fmt, Args);
                     error   -> error_logger:error_msg(Fmt, Args)
                 end
    end,
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
