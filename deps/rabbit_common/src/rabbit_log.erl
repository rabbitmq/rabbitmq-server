%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_log).

-export([log/3, log/4]).
-export([debug/1, debug/2, debug/3,
         info/1, info/2, info/3,
         notice/1, notice/2, notice/3,
         warning/1, warning/2, warning/3,
         error/1, error/2, error/3,
         critical/1, critical/2, critical/3,
         alert/1, alert/2, alert/3,
         emergency/1, emergency/2, emergency/3,
         none/1, none/2, none/3]).

-include("logging.hrl").
-include_lib("kernel/include/logger.hrl").

-compile({no_auto_import, [error/2, error/3]}).

%%----------------------------------------------------------------------------
%% This module is deprecated and only here for backwards compatibility.
%% New code should use Erlang logger directly, usually through ?LOG_* macros.
%%----------------------------------------------------------------------------

-deprecated({log, 3, "Use logger:log/3 instead"}).
-deprecated({log, 4, "Use logger:log/4 instead"}).

-deprecated({debug, 1, "Use ?LOG_DEBUG instead"}).
-deprecated({debug, 2, "Use ?LOG_DEBUG instead"}).
-deprecated({debug, 3, "Use ?LOG_DEBUG instead"}).

-deprecated({info, 1, "Use ?LOG_INFO instead"}).
-deprecated({info, 2, "Use ?LOG_INFO instead"}).
-deprecated({info, 3, "Use ?LOG_INFO instead"}).

-deprecated({notice, 1, "Use ?LOG_NOTICE instead"}).
-deprecated({notice, 2, "Use ?LOG_NOTICE instead"}).
-deprecated({notice, 3, "Use ?LOG_NOTICE instead"}).

-deprecated({warning, 1, "Use ?LOG_WARNING instead"}).
-deprecated({warning, 2, "Use ?LOG_WARNING instead"}).
-deprecated({warning, 3, "Use ?LOG_WARNING instead"}).

-deprecated({error, 1, "Use ?LOG_ERROR instead"}).
-deprecated({error, 2, "Use ?LOG_ERROR instead"}).
-deprecated({error, 3, "Use ?LOG_ERROR instead"}).

-deprecated({critical, 1, "Use ?LOG_CRITICAL instead"}).
-deprecated({critical, 2, "Use ?LOG_CRITICAL instead"}).
-deprecated({critical, 3, "Use ?LOG_CRITICAL instead"}).

-deprecated({emergency, 1, "Use ?LOG_EMERGENCY instead"}).
-deprecated({emergency, 2, "Use ?LOG_EMERGENCY instead"}).
-deprecated({emergency, 3, "Use ?LOG_EMERGENCY instead"}).

-deprecated({none, 1, "Deprecated"}).
-deprecated({none, 2, "Deprecated"}).
-deprecated({none, 3, "Deprecated"}).

%%----------------------------------------------------------------------------

-type category() :: atom().

-spec debug(string()) -> 'ok'.
-spec debug(string(), [any()]) -> 'ok'.
-spec debug(pid() | [tuple()], string(), [any()]) -> 'ok'.
-spec info(string()) -> 'ok'.
-spec info(string(), [any()]) -> 'ok'.
-spec info(pid() | [tuple()], string(), [any()]) -> 'ok'.
-spec notice(string()) -> 'ok'.
-spec notice(string(), [any()]) -> 'ok'.
-spec notice(pid() | [tuple()], string(), [any()]) -> 'ok'.
-spec warning(string()) -> 'ok'.
-spec warning(string(), [any()]) -> 'ok'.
-spec warning(pid() | [tuple()], string(), [any()]) -> 'ok'.
-spec error(string()) -> 'ok'.
-spec error(string(), [any()]) -> 'ok'.
-spec error(pid() | [tuple()], string(), [any()]) -> 'ok'.
-spec critical(string()) -> 'ok'.
-spec critical(string(), [any()]) -> 'ok'.
-spec critical(pid() | [tuple()], string(), [any()]) -> 'ok'.
-spec alert(string()) -> 'ok'.
-spec alert(string(), [any()]) -> 'ok'.
-spec alert(pid() | [tuple()], string(), [any()]) -> 'ok'.
-spec emergency(string()) -> 'ok'.
-spec emergency(string(), [any()]) -> 'ok'.
-spec emergency(pid() | [tuple()], string(), [any()]) -> 'ok'.
-spec none(string()) -> 'ok'.
-spec none(string(), [any()]) -> 'ok'.
-spec none(pid() | [tuple()], string(), [any()]) -> 'ok'.

%%----------------------------------------------------------------------------

-spec log(category(), logger:level(), string()) -> 'ok'.
log(Category, Level, Fmt) -> log(Category, Level, Fmt, []).

-spec log(category(), logger:level(), string(), [any()]) -> 'ok'.
log(default, Level, Fmt, Args) when is_list(Args) ->
    logger:log(Level, Fmt, Args, #{domain => ?RMQLOG_DOMAIN_GLOBAL});
log(Category, Level, Fmt, Args) when is_list(Args) ->
    logger:log(Level, Fmt, Args, #{domain => ?DEFINE_RMQLOG_DOMAIN(Category)}).

debug(Format) -> debug(Format, []).
debug(Format, Args) -> debug(self(), Format, Args).
debug(Pid, Format, Args) ->
    ?LOG_DEBUG(Format, Args, #{pid => Pid,
                               domain => ?RMQLOG_DOMAIN_GLOBAL}).

info(Format) -> info(Format, []).
info(Format, Args) -> info(self(), Format, Args).
info(Pid, Format, Args) ->
    ?LOG_INFO(Format, Args, #{pid => Pid,
                              domain => ?RMQLOG_DOMAIN_GLOBAL}).

notice(Format) -> notice(Format, []).
notice(Format, Args) -> notice(self(), Format, Args).
notice(Pid, Format, Args) ->
    ?LOG_NOTICE(Format, Args, #{pid => Pid,
                                domain => ?RMQLOG_DOMAIN_GLOBAL}).

warning(Format) -> warning(Format, []).
warning(Format, Args) -> warning(self(), Format, Args).
warning(Pid, Format, Args) ->
    ?LOG_WARNING(Format, Args, #{pid => Pid,
                                 domain => ?RMQLOG_DOMAIN_GLOBAL}).

error(Format) -> error(Format, []).
error(Format, Args) -> error(self(), Format, Args).
error(Pid, Format, Args) ->
    ?LOG_ERROR(Format, Args, #{pid => Pid,
                               domain => ?RMQLOG_DOMAIN_GLOBAL}).

critical(Format) -> critical(Format, []).
critical(Format, Args) -> critical(self(), Format, Args).
critical(Pid, Format, Args) ->
    ?LOG_CRITICAL(Format, Args, #{pid => Pid,
                                  domain => ?RMQLOG_DOMAIN_GLOBAL}).

alert(Format) -> alert(Format, []).
alert(Format, Args) -> alert(self(), Format, Args).
alert(Pid, Format, Args) ->
    ?LOG_ALERT(Format, Args, #{pid => Pid,
                               domain => ?RMQLOG_DOMAIN_GLOBAL}).

emergency(Format) -> emergency(Format, []).
emergency(Format, Args) -> emergency(self(), Format, Args).
emergency(Pid, Format, Args) ->
    ?LOG_EMERGENCY(Format, Args, #{pid => Pid,
                                   domain => ?RMQLOG_DOMAIN_GLOBAL}).

none(_Format) -> ok.
none(_Format, _Args) -> ok.
none(_Pid, _Format, _Args) -> ok.
