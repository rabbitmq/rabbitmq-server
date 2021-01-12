%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Compatibility module for the old Lager-based logging API.
-module(rabbit_log_shovel).

-export([debug/1, debug/2, debug/3,
         info/1, info/2, info/3,
         notice/1, notice/2, notice/3,
         warning/1, warning/2, warning/3,
         error/1, error/2, error/3,
         critical/1, critical/2, critical/3,
         alert/1, alert/2, alert/3,
         emergency/1, emergency/2, emergency/3,
         none/1, none/2, none/3]).

-include("include/logging.hrl").

-compile({no_auto_import, [error/2]}).

%%----------------------------------------------------------------------------

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

debug(Format) -> debug(Format, []).
debug(Format, Args) -> debug(self(), Format, Args).
debug(Pid, Format, Args) ->
    logger:debug(Format, Args, #{pid => Pid,
                                 domain => ?RMQLOG_DOMAIN_SHOVEL}).

info(Format) -> info(Format, []).
info(Format, Args) -> info(self(), Format, Args).
info(Pid, Format, Args) ->
    logger:info(Format, Args, #{pid => Pid,
                                domain => ?RMQLOG_DOMAIN_SHOVEL}).

notice(Format) -> notice(Format, []).
notice(Format, Args) -> notice(self(), Format, Args).
notice(Pid, Format, Args) ->
    logger:notice(Format, Args, #{pid => Pid,
                                  domain => ?RMQLOG_DOMAIN_SHOVEL}).

warning(Format) -> warning(Format, []).
warning(Format, Args) -> warning(self(), Format, Args).
warning(Pid, Format, Args) ->
    logger:warning(Format, Args, #{pid => Pid,
                                   domain => ?RMQLOG_DOMAIN_SHOVEL}).

error(Format) -> error(Format, []).
error(Format, Args) -> error(self(), Format, Args).
error(Pid, Format, Args) ->
    logger:error(Format, Args, #{pid => Pid,
                                 domain => ?RMQLOG_DOMAIN_SHOVEL}).

critical(Format) -> critical(Format, []).
critical(Format, Args) -> critical(self(), Format, Args).
critical(Pid, Format, Args) ->
    logger:critical(Format, Args, #{pid => Pid,
                                    domain => ?RMQLOG_DOMAIN_SHOVEL}).

alert(Format) -> alert(Format, []).
alert(Format, Args) -> alert(self(), Format, Args).
alert(Pid, Format, Args) ->
    logger:alert(Format, Args, #{pid => Pid,
                                 domain => ?RMQLOG_DOMAIN_SHOVEL}).

emergency(Format) -> emergency(Format, []).
emergency(Format, Args) -> emergency(self(), Format, Args).
emergency(Pid, Format, Args) ->
    logger:emergency(Format, Args, #{pid => Pid,
                                     domain => ?RMQLOG_DOMAIN_SHOVEL}).

none(_Format) -> ok.
none(_Format, _Args) -> ok.
none(_Pid, _Format, _Args) -> ok.
