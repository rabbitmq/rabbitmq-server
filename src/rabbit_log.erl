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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_log).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([log/3, log/4, info/1, info/2, warning/1, warning/2, error/1, error/2]).

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([level/0]).

-type(category() :: atom()).
-type(level() :: 'info' | 'warning' | 'error').

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).

-spec(log/3 :: (category(), level(), string()) -> 'ok').
-spec(log/4 :: (category(), level(), string(), [any()]) -> 'ok').
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
log(Category, Level, Fmt) -> log(Category, Level, Fmt, []).

log(Category, Level, Fmt, Args) when is_list(Args) ->
    gen_server:cast(?SERVER, {log, Category, Level, Fmt, Args}).

info(Fmt)          -> log(default, info,    Fmt).
info(Fmt, Args)    -> log(default, info,    Fmt, Args).
warning(Fmt)       -> log(default, warning, Fmt).
warning(Fmt, Args) -> log(default, warning, Fmt, Args).
error(Fmt)         -> log(default, error,   Fmt).
error(Fmt, Args)   -> log(default, error,   Fmt, Args).

%%--------------------------------------------------------------------

init([]) ->
    {ok, CatLevelList} = application:get_env(log_levels),
    CatLevels = [{Cat, level(Level)} || {Cat, Level} <- CatLevelList],
    {ok, orddict:from_list(CatLevels)}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({log, Category, Level, Fmt, Args}, CatLevels) ->
    CatLevel = case orddict:find(Category, CatLevels) of
                   {ok, L} -> L;
                   error   -> level(info)
               end,
    case level(Level) =< CatLevel of
        false -> ok;
        true  -> (case Level of
                      info    -> fun error_logger:info_msg/2;
                      warning -> fun error_logger:warning_msg/2;
                      error   -> fun error_logger:error_msg/2
                  end)(Fmt, Args)
    end,
    {noreply, CatLevels};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------

level(info)    -> 3;
level(warning) -> 2;
level(error)   -> 1;
level(none)    -> 0.
