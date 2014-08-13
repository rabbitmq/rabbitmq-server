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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_log).

-export([log/3, log/4, info/1, info/2, warning/1, warning/2, error/1, error/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([level/0]).

-type(category() :: atom()).
-type(level() :: 'info' | 'warning' | 'error').

-spec(log/3 :: (category(), level(), string()) -> 'ok').
-spec(log/4 :: (category(), level(), string(), [any()]) -> 'ok').

-spec(info/1    :: (string()) -> 'ok').
-spec(info/2    :: (string(), [any()]) -> 'ok').
-spec(warning/1 :: (string()) -> 'ok').
-spec(warning/2 :: (string(), [any()]) -> 'ok').
-spec(error/1   :: (string()) -> 'ok').
-spec(error/2   :: (string(), [any()]) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

log(Category, Level, Fmt) -> log(Category, Level, Fmt, []).

log(Category, Level, Fmt, Args) when is_list(Args) ->
    case level(Level) =< catlevel(Category) of
        false -> ok;
        true  -> (case Level of
                      info    -> fun error_logger:info_msg/2;
                      warning -> fun error_logger:warning_msg/2;
                      error   -> fun error_logger:error_msg/2
                  end)(Fmt, Args)
    end.

info(Fmt)          -> log(default, info,    Fmt).
info(Fmt, Args)    -> log(default, info,    Fmt, Args).
warning(Fmt)       -> log(default, warning, Fmt).
warning(Fmt, Args) -> log(default, warning, Fmt, Args).
error(Fmt)         -> log(default, error,   Fmt).
error(Fmt, Args)   -> log(default, error,   Fmt, Args).

catlevel(Category) ->
    {ok, CatLevelList} = application:get_env(rabbit, log_levels),
    level(proplists:get_value(Category, CatLevelList, info)).

%%--------------------------------------------------------------------

level(info)    -> 3;
level(warning) -> 2;
level(error)   -> 1;
level(none)    -> 0.
