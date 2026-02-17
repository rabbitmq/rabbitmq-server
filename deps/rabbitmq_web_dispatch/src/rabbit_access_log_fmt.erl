%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% OTP Logger formatter for HTTP access logs.
%% Outputs the pre-formatted message with a trailing newline.

-module(rabbit_access_log_fmt).

-export([format/2, check_config/1]).

-spec format(logger:log_event(), logger:formatter_config()) -> unicode:chardata().
format(#{msg := {string, Msg}}, _Config) ->
    [Msg, $\n];
format(#{msg := {report, #{formatted := Msg}}}, _Config) ->
    [Msg, $\n];
format(#{msg := {report, _}}, _Config) ->
    [];
format(#{msg := {Format, Args}}, _Config) ->
    [io_lib:format(Format, Args), $\n];
format(_, _Config) ->
    [].

-spec check_config(logger:formatter_config()) -> ok | {error, term()}.
check_config(_Config) ->
    ok.
