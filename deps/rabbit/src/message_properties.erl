
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(message_properties).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([
  new/0,
  new/1,
  new/4,
  record_version_to_use/0,
  fields/0,
  fields/1,
  upgrade/1,
  upgrade_to/2,
  get_expiry/1,
  get_needs_confirming/1,
  get_size/1,
  get_delivery_count/1,
  set_expiry/2,
  set_needs_confirming/2,
  set_delivery_count/2
]).

-define(record_version, message_properties_v2).

-type(expiry() :: pos_integer() | '_').

-type(confirm() :: boolean() | '_').

-type(size() :: pos_integer() | '_').

-type(delivery_count() :: pos_integer() | '_').

-type message_properties() :: message_properties_v1:message_properties_v1() |
                                message_properties_v2().

-record(message_properties, {
    expiry :: expiry(),
    needs_confirming :: confirm(),
    size :: size(),
    delivery_count :: delivery_count()}).

-type(message_properties_v2() ::
        #message_properties{expiry           :: expiry(),
                            needs_confirming :: confirm(),
                            size             :: size(),
                            delivery_count   :: delivery_count()}).

-type message_properties_pattern() :: message_properties_v1:message_properties_v1_pattern() |
                                 message_properties_v2_pattern().

-type message_properties_v2_pattern() :: #message_properties{
                                                  expiry :: expiry(),
                                                  needs_confirming :: confirm(),
                                                  size :: size(),
                                                  delivery_count :: delivery_count()
                                                 }.

-export_type([expiry/0, confirm/0, size/0, delivery_count/0,
              message_properties_pattern/0,
              message_properties_v2_pattern/0]).

-spec new() -> message_properties().
new() ->
    case record_version_to_use() of
        ?record_version ->
            #message_properties{
                expiry = undefined,
                needs_confirming = false,
                size = 0,
                delivery_count = 0
            };
        _ ->
            message_properties_v1:new()
    end.

-spec new(tuple()) -> message_properties().
new({size, Size}) ->
    new(undefined, false, Size, 0).

-spec new(expiry(), confirm(), size(), delivery_count()) -> message_properties().
new(Expiry, Confirm, Size, DC) ->
    case record_version_to_use() of
        ?record_version ->
            #message_properties{
                expiry = Expiry,
                needs_confirming = Confirm,
                size = Size,
                delivery_count = DC
            };
        _ ->
            message_properties_v1:new(Expiry, Confirm, Size, DC)
    end.

-spec record_version_to_use() -> message_properties_v1 | message_properties_v2.
record_version_to_use() ->
    case rabbit_feature_flags:is_enabled(classic_delivery_limits) of
        true  -> ?record_version;
        false -> message_properties_v1:record_version_to_use()
    end.

-spec fields() -> list().
fields() ->
    case record_version_to_use() of
        ?record_version -> fields(?record_version);
        _               -> message_properties_v1:fields()
    end.

-spec fields(atom()) -> list().
fields(?record_version) -> record_info(fields, message_properties);
fields(Version)         -> message_properties_v1:fields(Version).

-spec upgrade(message_properties()) -> message_properties().
upgrade(#message_properties{} = MessageProps) -> MessageProps;
upgrade(OldMessageProps) -> upgrade_to(record_version_to_use(), OldMessageProps).

-spec upgrade_to
(message_properties_v2, message_properties()) -> message_properties_v2();
(message_properties_v1, message_properties_v1:message_properties_v1()) ->
    message_properties_v1:message_properties_v1().

upgrade_to(?record_version, #message_properties{} = MessageProps) ->
    MessageProps;
upgrade_to(?record_version, OldMessageProps) ->
    Fields = erlang:tuple_to_list(OldMessageProps) ++ [0],
    #message_properties{} = erlang:list_to_tuple(Fields);
upgrade_to(Version, OldMessageProps) ->
    message_properties_v1:upgrade_to(Version, OldMessageProps).

-spec get_expiry(message_properties()) -> expiry().
get_expiry(#message_properties{expiry = Value}) -> Value;
get_expiry(MessageProps) -> message_properties_v1:get_expiry(MessageProps).

-spec get_needs_confirming(message_properties()) -> confirm().
get_needs_confirming(#message_properties{needs_confirming = Value}) -> Value;
get_needs_confirming(MessageProps) -> message_properties_v1:get_needs_confirming(MessageProps).

-spec get_size(message_properties()) -> integer().
get_size(#message_properties{size = Value}) -> Value;
get_size(MessageProps) -> message_properties_v1:get_size(MessageProps).

-spec get_delivery_count(message_properties()) -> integer().
get_delivery_count(#message_properties{delivery_count = Value}) -> Value;
get_delivery_count(MessageProps) -> message_properties_v1:get_delivery_count(MessageProps).

-spec set_expiry(message_properties(), expiry()) -> message_properties().
set_expiry(#message_properties{} = MessageProps, Expiry) ->
    MessageProps#message_properties{expiry = Expiry};
set_expiry(MessageProps, Expiry) ->
    message_properties_v1:set_expiry(MessageProps, Expiry).

-spec set_needs_confirming(message_properties(), confirm()) -> message_properties().
set_needs_confirming(#message_properties{} = MessageProps, Confirm) ->
    MessageProps#message_properties{needs_confirming = Confirm};
set_needs_confirming(MessageProps, Confirm) ->
    message_properties_v1:set_needs_confirming(MessageProps, Confirm).

-spec set_delivery_count(message_properties(), delivery_count()) -> message_properties().
set_delivery_count(#message_properties{} = MessageProps, DeliveryCount) ->
    MessageProps#message_properties{delivery_count = DeliveryCount};
set_delivery_count(MessageProps, DeliveryCount) ->
    message_properties_v1:set_delivery_count(MessageProps, DeliveryCount).
