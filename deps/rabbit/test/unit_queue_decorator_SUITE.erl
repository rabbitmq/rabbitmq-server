%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_queue_decorator_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          select
        ]}
    ].

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

%% A queue's decorators field can legitimately be `undefined` or `none` (see
%% the amqqueue record type). For example, quorum queues migrated from Mnesia
%% to Khepri retain `decorators = undefined`. select/1 must tolerate those
%% values rather than crash with a `bad_generator` error.
select(_Config) ->
    [] = rabbit_queue_decorator:select(undefined),
    [] = rabbit_queue_decorator:select(none),
    [] = rabbit_queue_decorator:select([]),
    %% Existing modules are kept, non-existing ones are filtered out.
    [?MODULE] = rabbit_queue_decorator:select([?MODULE]),
    [?MODULE] = rabbit_queue_decorator:select([?MODULE, definitely_not_a_real_module]),
    passed.
