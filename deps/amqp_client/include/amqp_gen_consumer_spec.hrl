%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2014 GoPivotal, Inc.  All rights reserved.
%%

-include("amqp_client.hrl").

-ifndef(edoc).
-type(state() :: any()).
-type(consume() :: #'basic.consume'{}).
-type(consume_ok() :: #'basic.consume_ok'{}).
-type(cancel() :: #'basic.cancel'{}).
-type(cancel_ok() :: #'basic.cancel_ok'{}).
-type(deliver() :: #'basic.deliver'{}).
-type(from() :: any()).
-type(reason() :: any()).
-type(ok_error() :: {ok, state()} | {error, reason(), state()}).

-spec(init/1 :: ([any()]) -> {ok, state()}).
-spec(handle_consume/3 :: (consume(), pid(), state()) -> ok_error()).
-spec(handle_consume_ok/3 :: (consume_ok(), consume(), state()) ->
                                  ok_error()).
-spec(handle_cancel/2 :: (cancel(), state()) -> ok_error()).
-spec(handle_server_cancel/2 :: (cancel(), state()) -> ok_error()).
-spec(handle_cancel_ok/3 :: (cancel_ok(), cancel(), state()) -> ok_error()).
-spec(handle_deliver/3 :: (deliver(), #amqp_msg{}, state()) -> ok_error()).
-spec(handle_info/2 :: (any(), state()) -> ok_error()).
-spec(handle_call/3 :: (any(), from(), state()) ->
                           {reply, any(), state()} | {noreply, state()} |
                            {error, reason(), state()}).
-spec(terminate/2 :: (any(), state()) -> state()).
-endif.
