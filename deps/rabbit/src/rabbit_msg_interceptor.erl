%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_msg_interceptor).

%% client API
-export([intercept_incoming/2,
         intercept_outgoing/2,
         add/1,
         remove/1]).
%% helpers for behaviour implementations
-export([set_annotation/4]).

%% same protocol names as output by Prometheus endpoint
-type protocol() :: amqp091 | amqp10 | mqtt310 | mqtt311 | mqtt50.
-type context() :: #{protocol := protocol(),
                     vhost := rabbit_types:vhost(),
                     username := rabbit_types:username(),
                     connection_name := binary(),
                     atom() => term()}.
-type config() :: #{atom() => term()}.
-type interceptor() :: {module(), config()}.
-type interceptors() :: [interceptor()].
-type stage() :: incoming | outgoing.

-define(KEY, message_interceptors).

-export_type([context/0]).

-callback intercept(mc:state(), context(), stage(), config()) ->
    mc:state().

-spec intercept_incoming(mc:state(), context()) ->
    mc:state().
intercept_incoming(Msg, Ctx) ->
    intercept(Msg, Ctx, incoming).

-spec intercept_outgoing(mc:state(), context()) ->
    mc:state().
intercept_outgoing(Msg, Ctx) ->
    intercept(Msg, Ctx, outgoing).

intercept(Msg, Ctx, Stage) ->
    Interceptors = persistent_term:get(?KEY),
    lists:foldl(fun({Mod, Cfg}, Msg0) ->
                        Mod:intercept(Msg0, Ctx, Stage, Cfg)
                end, Msg, Interceptors).

-spec set_annotation(mc:state(), mc:ann_key(), mc:ann_value(),
                     Overwrite :: boolean()) ->
    mc:state().
set_annotation(Msg, Key, Value, true) ->
    mc:set_annotation(Key, Value, Msg);
set_annotation(Msg, Key, Value, false) ->
    case mc:x_header(Key, Msg) of
        undefined ->
            mc:set_annotation(Key, Value, Msg);
        _ ->
            Msg
    end.

-spec add(interceptors()) -> ok.
add(Interceptors) ->
    %% validation
    lists:foreach(fun({Mod, #{}}) ->
                          case erlang:function_exported(Mod, intercept, 4) of
                              true -> ok;
                              false -> error(Mod)
                          end
                  end, Interceptors),
    persistent_term:put(?KEY, persistent_term:get(?KEY, []) ++ Interceptors).

-spec remove(interceptors()) -> ok.
remove(Interceptors) ->
    persistent_term:put(?KEY, persistent_term:get(?KEY, []) -- Interceptors).
