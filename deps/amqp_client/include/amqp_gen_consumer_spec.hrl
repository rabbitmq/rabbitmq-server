%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-include("amqp_client.hrl").

-type state() :: any().
-type consume() :: #'basic.consume'{}.
-type consume_ok() :: #'basic.consume_ok'{}.
-type cancel() :: #'basic.cancel'{}.
-type cancel_ok() :: #'basic.cancel_ok'{}.
-type deliver() :: #'basic.deliver'{}.
-type from() :: any().
-type reason() :: any().
-type ok_error() :: {ok, state()} | {error, reason(), state()}.

-spec init([any()]) -> {ok, state()}.
-spec handle_consume(consume(), pid(), state()) -> ok_error().
-spec handle_consume_ok(consume_ok(), consume(), state()) ->
                                  ok_error().
-spec handle_cancel(cancel(), state()) -> ok_error().
-spec handle_server_cancel(cancel(), state()) -> ok_error().
-spec handle_cancel_ok(cancel_ok(), cancel(), state()) -> ok_error().
-spec handle_deliver(deliver(), #amqp_msg{}, state()) -> ok_error().
-spec handle_info(any(), state()) -> ok_error().
-spec handle_call(any(), from(), state()) ->
                           {reply, any(), state()} | {noreply, state()} |
                            {error, reason(), state()}.
-spec terminate(any(), state()) -> state().
