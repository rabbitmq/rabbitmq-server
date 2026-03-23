%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_queue_type_common).

-export([get_queue_type/2, default/0]).

-spec get_queue_type(rabbit_framing:amqp_table(), module()) -> module().
%% This version should be used together with 'rabbit_vhost:default_queue_type/{1,2}'.
get_queue_type([], DefaultQueueType) ->
    discover(DefaultQueueType);
get_queue_type(Args, DefaultQueueType) ->
    case rabbit_misc:table_lookup(Args, <<"x-queue-type">>) of
        undefined                  -> discover(DefaultQueueType);
        {longstr, undefined}       -> discover(DefaultQueueType);
        {longstr, <<"undefined">>} -> discover(DefaultQueueType);
        {_, V}                     -> discover(V)
    end.

%% ------------------------------------------------------------------

-spec default() -> module().
default() ->
    rabbit_data_coercion:to_atom(
        application:get_env(rabbit, default_queue_type, rabbit_classic_queue)).

discover(TypeDescriptor) ->
    {ok, Module} = rabbit_registry:lookup_type_module(queue, TypeDescriptor),
    Module.
