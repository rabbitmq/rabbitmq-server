%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% Observe-only extension point at the queue boundary, symmetric to the two
%% queue-side events a message goes through:
%%
%%   * enqueue: a message has been delivered into a queue (after routing),
%%   * settle:  message ids have been settled at a queue (ack, requeue,
%%              discard or modify).
%%
%% Plugins register a module via add/1. The callback results are ignored:
%% interceptors cannot alter or block delivery or settlement.
%%
%% This is the queue-boundary counterpart of rabbit_msg_interceptor, which
%% intercepts (and may transform) messages at the channel/protocol boundary
%% on publish (incoming) and delivery to a consumer (outgoing). The hooks here
%% fire on the queue side and carry no message body for settlement, only the
%% settlement arguments.
-module(rabbit_queue_interceptor).

-export([enabled/0,
         enqueue/2,
         settle/4,
         add/1,
         remove/1]).

-type config() :: #{atom() => term()}.
-type interceptor() :: {module(), config()}.
-type interceptors() :: [interceptor()].

-define(KEY, queue_interceptors).

-export_type([config/0]).

-callback intercept_enqueue(QName :: rabbit_amqqueue:name(),
                            Message :: mc:state(),
                            Config :: config()) ->
    ok.

-callback intercept_settle(QName :: rabbit_amqqueue:name(),
                           Op :: rabbit_queue_type:settle_op(),
                           CTag :: rabbit_types:ctag(),
                           MsgIds :: [non_neg_integer()],
                           Config :: config()) ->
    ok.

%% A plugin only needs to implement the stage(s) it cares about.
-optional_callbacks([intercept_enqueue/3,
                     intercept_settle/5]).

%% Cheap gate (a single persistent_term read) so callers on the hot delivery
%% path can skip per-queue work when no plugin is registered.
-spec enabled() -> boolean().
enabled() ->
    interceptors() =/= [].

-spec enqueue(rabbit_amqqueue:name(), mc:state()) -> ok.
enqueue(QName, Message) ->
    lists:foreach(fun({Mod, Cfg}) ->
                          case erlang:function_exported(Mod, intercept_enqueue, 3) of
                              true ->
                                  _ = Mod:intercept_enqueue(QName, Message, Cfg),
                                  ok;
                              false ->
                                  ok
                          end
                  end, interceptors()).

-spec settle(rabbit_amqqueue:name(), rabbit_queue_type:settle_op(),
             rabbit_types:ctag(), [non_neg_integer()]) -> ok.
settle(QName, Op, CTag, MsgIds) ->
    lists:foreach(fun({Mod, Cfg}) ->
                          case erlang:function_exported(Mod, intercept_settle, 5) of
                              true ->
                                  _ = Mod:intercept_settle(QName, Op, CTag, MsgIds, Cfg),
                                  ok;
                              false ->
                                  ok
                          end
                  end, interceptors()).

-spec add(interceptors()) -> ok.
add(Interceptors) ->
    %% validation: a module must implement at least one stage
    lists:foreach(fun({Mod, #{}}) ->
                          case erlang:function_exported(Mod, intercept_enqueue, 3) orelse
                               erlang:function_exported(Mod, intercept_settle, 5) of
                              true -> ok;
                              false -> error(Mod)
                          end
                  end, Interceptors),
    persistent_term:put(?KEY, interceptors() ++ Interceptors).

-spec remove(interceptors()) -> ok.
remove(Interceptors) ->
    persistent_term:put(?KEY, interceptors() -- Interceptors).

%% Default to [] because, unlike message_interceptors, this key is not
%% initialised at boot: it stays unset until a plugin registers.
interceptors() ->
    persistent_term:get(?KEY, []).
