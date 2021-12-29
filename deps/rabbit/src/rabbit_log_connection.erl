%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Compatibility module for the old Lager-based logging API.
-module(rabbit_log_connection).

-export([debug/1, debug/2, debug/3,
         info/1, info/2, info/3,
         notice/1, notice/2, notice/3,
         warning/1, warning/2, warning/3,
         error/1, error/2, error/3,
         critical/1, critical/2, critical/3,
         alert/1, alert/2, alert/3,
         emergency/1, emergency/2, emergency/3,
         none/1, none/2, none/3]).

-include_lib("rabbit_common/include/logging.hrl").

-compile({no_auto_import, [error/2, error/3]}).

-spec debug(string()) -> 'ok'.
debug(Format) -> debug(Format, []).

-spec debug(string(), [any()]) -> 'ok'.
debug(Format, Args) -> debug(self(), Format, Args).

-spec debug(pid() | [tuple()], string(), [any()]) -> 'ok'.
debug(Pid, Format, Args) ->
    logger:debug(Format, Args, #{pid => Pid,
                                 domain => ?RMQLOG_DOMAIN_CONN}).

-spec info(string()) -> 'ok'.
info(Format) -> info(Format, []).

-spec info(string(), [any()]) -> 'ok'.
info(Format, Args) -> info(self(), Format, Args).

-spec info(pid() | [tuple()], string(), [any()]) -> 'ok'.
info(Pid, Format, Args) ->
    logger:info(Format, Args, #{pid => Pid,
                                domain => ?RMQLOG_DOMAIN_CONN}).

-spec notice(string()) -> 'ok'.
notice(Format) -> notice(Format, []).

-spec notice(string(), [any()]) -> 'ok'.
notice(Format, Args) -> notice(self(), Format, Args).

-spec notice(pid() | [tuple()], string(), [any()]) -> 'ok'.
notice(Pid, Format, Args) ->
    logger:notice(Format, Args, #{pid => Pid,
                                  domain => ?RMQLOG_DOMAIN_CONN}).

-spec warning(string()) -> 'ok'.
warning(Format) -> warning(Format, []).

-spec warning(string(), [any()]) -> 'ok'.
warning(Format, Args) -> warning(self(), Format, Args).

-spec warning(pid() | [tuple()], string(), [any()]) -> 'ok'.
warning(Pid, Format, Args) ->
    logger:warning(Format, Args, #{pid => Pid,
                                   domain => ?RMQLOG_DOMAIN_CONN}).

-spec error(string()) -> 'ok'.
error(Format) -> error(Format, []).

-spec error(string(), [any()]) -> 'ok'.
error(Format, Args) -> error(self(), Format, Args).

-spec error(pid() | [tuple()], string(), [any()]) -> 'ok'.
error(Pid, Format, Args) ->
    logger:error(Format, Args, #{pid => Pid,
                                 domain => ?RMQLOG_DOMAIN_CONN}).

-spec critical(string()) -> 'ok'.
critical(Format) -> critical(Format, []).

-spec critical(string(), [any()]) -> 'ok'.
critical(Format, Args) -> critical(self(), Format, Args).

-spec critical(pid() | [tuple()], string(), [any()]) -> 'ok'.
critical(Pid, Format, Args) ->
    logger:critical(Format, Args, #{pid => Pid,
                                    domain => ?RMQLOG_DOMAIN_CONN}).

-spec alert(string()) -> 'ok'.
alert(Format) -> alert(Format, []).

-spec alert(string(), [any()]) -> 'ok'.
alert(Format, Args) -> alert(self(), Format, Args).

-spec alert(pid() | [tuple()], string(), [any()]) -> 'ok'.
alert(Pid, Format, Args) ->
    logger:alert(Format, Args, #{pid => Pid,
                                 domain => ?RMQLOG_DOMAIN_CONN}).

-spec emergency(string()) -> 'ok'.
emergency(Format) -> emergency(Format, []).

-spec emergency(string(), [any()]) -> 'ok'.
emergency(Format, Args) -> emergency(self(), Format, Args).

-spec emergency(pid() | [tuple()], string(), [any()]) -> 'ok'.
emergency(Pid, Format, Args) ->
    logger:emergency(Format, Args, #{pid => Pid,
                                     domain => ?RMQLOG_DOMAIN_CONN}).

-spec none(string()) -> 'ok'.
none(_Format) -> ok.

-spec none(string(), [any()]) -> 'ok'.
none(_Format, _Args) -> ok.

-spec none(pid() | [tuple()], string(), [any()]) -> 'ok'.
none(_Pid, _Format, _Args) -> ok.
