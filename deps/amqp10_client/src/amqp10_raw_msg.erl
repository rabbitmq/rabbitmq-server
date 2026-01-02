%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(amqp10_raw_msg).

-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("amqp10_common/include/amqp10_types.hrl").

%% Just for AMQP10 shovel usage. It avoids the binary <-> amqp10 <-> mc
%% conversion, with all the unnecessary encoding/decoding steps.
%% It allows for just binary <-> mc conversion, as the payload is stored as is

-export([new/3,
         settled/1,
         delivery_tag/1,
         payload/1,
         handle/1,
         set_handle/2,
         transfer/1,
         is/1]).

-record(amqp10_raw_msg,
        {settled :: boolean(),
         delivery_tag :: non_neg_integer(),
         payload :: binary(),
         handle :: non_neg_integer() | undefined
        }).

-opaque amqp10_raw_msg() :: #amqp10_raw_msg{}.

-export_type([amqp10_raw_msg/0]).

-spec new(boolean(), non_neg_integer(), binary()) ->
          amqp10_raw_msg().
new(Settled, DeliveryTag, Payload) ->
    #amqp10_raw_msg{settled = Settled,
                    delivery_tag = DeliveryTag,
                    payload = Payload}.

-spec settled(amqp10_raw_msg()) -> boolean().
settled(#amqp10_raw_msg{settled = Settled}) ->
    Settled.

-spec delivery_tag(amqp10_raw_msg()) -> non_neg_integer().
delivery_tag(#amqp10_raw_msg{delivery_tag = DeliveryTag}) ->
    DeliveryTag.

-spec payload(amqp10_raw_msg()) -> binary().
payload(#amqp10_raw_msg{payload = Payload}) ->
    Payload.

-spec handle(amqp10_raw_msg()) -> non_neg_integer().
handle(#amqp10_raw_msg{handle = Handle}) ->
    Handle.

-spec set_handle(non_neg_integer(), amqp10_raw_msg()) ->
    amqp10_raw_msg().
set_handle(Handle, #amqp10_raw_msg{} = Msg) ->
    Msg#amqp10_raw_msg{handle = Handle}.

-spec transfer(amqp10_raw_msg()) -> #'v1_0.transfer'{}.
transfer(#amqp10_raw_msg{settled = Settled,
                         delivery_tag = DeliveryTag,
                         handle = Handle}) ->
    #'v1_0.transfer'{
       delivery_tag = {binary, rabbit_data_coercion:to_binary(DeliveryTag)},
       settled = Settled,
       handle = {uint, Handle},
       message_format = {uint, ?MESSAGE_FORMAT}}.

-spec is(term()) -> boolean().
is(Record) ->
    is_record(Record, amqp10_raw_msg).
