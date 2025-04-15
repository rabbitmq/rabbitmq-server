%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_message_interceptor).

%% client API
-export([intercept/3,
         add/2,
         remove/2]).
%% helpers for behaviour implementations
-export([set_annotation/4]).

%% same protocol names as output by Prometheus endpoint
-type protocol() :: amqp091 | amqp10 | mqtt310 | mqtt311 | mqtt50.
-type context() :: #{protocol := protocol(),
                     vhost := rabbit_types:vhost(),
                     username := rabbit_types:username(),
                     connection_name := binary(),
                     atom() => term()}.
-type group() :: incoming_message_interceptors |
                 outgoing_message_interceptors.
-type config() :: #{atom() => term()}.
-type interceptor() :: {module(), config()}.


-export_type([context/0]).

-callback intercept(mc:state(), context(), group(), config()) ->
    mc:state().

-spec intercept(mc:state(), context(), group()) ->
    mc:state().
intercept(Msg, Ctx, Group) ->
    Interceptors = persistent_term:get(Group, []),
    lists:foldl(fun({Mod, Config}, Msg0) ->
                        Mod:intercept(Msg0, Ctx, Group, Config)
                end, Msg, Interceptors).

-spec set_annotation(mc:state(), mc:ann_key(), mc:ann_value(), boolean()) ->
    mc:state().
set_annotation(Msg, Key, Value, Overwrite) ->
    case {mc:x_header(Key, Msg), Overwrite} of
        {Val, false} when Val =/= undefined ->
            Msg;
        _ ->
            mc:set_annotation(Key, Value, Msg)
    end.

-spec add([interceptor()], group()) -> ok.
add(Interceptors, Group) ->
    %% validation
    lists:foreach(fun({Mod, #{}}) ->
                          case erlang:function_exported(Mod, intercept, 4) of
                              true -> ok;
                              false -> error(Mod)
                          end
                  end, Interceptors),
    persistent_term:put(Group, persistent_term:get(Group, []) ++ Interceptors).

-spec remove([interceptor()], group()) -> ok.
remove(Interceptors, Group) ->
    persistent_term:put(Group, persistent_term:get(Group, []) -- Interceptors).
