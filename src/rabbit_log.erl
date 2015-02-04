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

-export([log/3, log/4, debug/1, debug/2, info/1, info/2, warning/1,
         warning/2, error/1, error/2]).
-export([with_local_io/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([level/0]).

-type(category() :: atom()).
-type(level() :: 'debug' | 'info' | 'warning' | 'error').

-spec(log/3 :: (category(), level(), string()) -> 'ok').
-spec(log/4 :: (category(), level(), string(), [any()]) -> 'ok').

-spec(debug/1   :: (string()) -> 'ok').
-spec(debug/2   :: (string(), [any()]) -> 'ok').
-spec(info/1    :: (string()) -> 'ok').
-spec(info/2    :: (string(), [any()]) -> 'ok').
-spec(warning/1 :: (string()) -> 'ok').
-spec(warning/2 :: (string(), [any()]) -> 'ok').
-spec(error/1   :: (string()) -> 'ok').
-spec(error/2   :: (string(), [any()]) -> 'ok').

-spec(with_local_io/1 :: (fun (() -> A)) -> A).

-endif.

%%----------------------------------------------------------------------------

log(Category, Level, Fmt) -> log(Category, Level, Fmt, []).

log(Category, Level, Fmt, Args) when is_list(Args) ->
    case level(Level) =< catlevel(Category) of
        false -> ok;
        true  -> F = case Level of
                         debug   -> fun error_logger:info_msg/2;
                         info    -> fun error_logger:info_msg/2;
                         warning -> fun error_logger:warning_msg/2;
                         error   -> fun error_logger:error_msg/2
                     end,
                 with_local_io(fun () -> F(Fmt, Args) end)
    end.

debug(Fmt)         -> log(default, debug,    Fmt).
debug(Fmt, Args)   -> log(default, debug,    Fmt, Args).
info(Fmt)          -> log(default, info,    Fmt).
info(Fmt, Args)    -> log(default, info,    Fmt, Args).
warning(Fmt)       -> log(default, warning, Fmt).
warning(Fmt, Args) -> log(default, warning, Fmt, Args).
error(Fmt)         -> log(default, error,   Fmt).
error(Fmt, Args)   -> log(default, error,   Fmt, Args).

catlevel(Category) ->
    %% We can get here as part of rabbitmqctl when it is impersonating
    %% a node; in which case the env will not be defined.
    CatLevelList = case application:get_env(rabbit, log_levels) of
                       {ok, L}   -> L;
                       undefined -> []
                   end,
    level(proplists:get_value(Category, CatLevelList, info)).

%%--------------------------------------------------------------------

level(debug)   -> 4;
level(info)    -> 3;
level(warning) -> 2;
level(error)   -> 1;
level(none)    -> 0.

%% Execute Fun using the IO system of the local node (i.e. the node on
%% which the code is executing). Since this is invoked for every log
%% message, we try to avoid unnecessarily churning group_leader/1.
with_local_io(Fun) ->
    GL = group_leader(),
    Node = node(),
    case node(GL) of
        Node -> Fun();
        _    -> group_leader(whereis(user), self()),
                try
                    Fun()
                after
                    group_leader(GL, self())
                end
    end.
