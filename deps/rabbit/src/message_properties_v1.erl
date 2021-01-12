%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(message_properties_v1).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([
  new/0,
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

-define(record_version, ?MODULE).

-record(message_properties, {
    expiry :: message_properties:expiry(),
    needs_confirming :: message_properties:confirm(),
    size :: message_properties:size()}).

-type message_properties() :: message_properties_v1().

-type message_properties_v1() ::
        #message_properties{expiry           :: message_properties:expiry(),
                            needs_confirming :: message_properties:confirm(),
                            size             :: message_properties:size()}.

-type message_properties_pattern() :: message_properties_v1_pattern().

-type message_properties_v1_pattern() ::
        #message_properties{expiry           :: message_properties:expiry(),
                            needs_confirming :: message_properties:confirm(),
                            size             :: message_properties:size()
                           }.

-export_type([message_properties/0,
              message_properties_v1/0,
              message_properties_pattern/0,
              message_properties_v1_pattern/0]).

-spec record_version_to_use() -> message_properties_v1.
record_version_to_use() ->
    ?record_version.

-spec new() -> message_properties().
new() ->
    #message_properties{
        expiry = undefined,
        needs_confirming = false,
        size = 0
    }.

-spec new(message_properties:expiry(), message_properties:confirm(),
          message_properties:size(), message_properties:delivery_count()) -> message_properties().
new(Expiry, Confirm, Size, _DC) ->
    #message_properties{
        expiry = Expiry,
        needs_confirming = Confirm,
        size = Size
    }.

-spec fields() -> list().
fields() -> fields(?record_version).

-spec fields(atom()) -> list().
fields(?record_version) -> record_info(fields, message_properties).

-spec upgrade(message_properties()) -> message_properties().
upgrade(#message_properties{} = MessageProps) -> MessageProps.

-spec upgrade_to(message_properties_v1, message_properties()) -> message_properties().
upgrade_to(?record_version, #message_properties{} = MessageProps) ->
    MessageProps.

-spec get_expiry(message_properties()) -> message_properties:expiry().
get_expiry(#message_properties{expiry = Value}) -> Value.

-spec get_needs_confirming(message_properties()) -> message_properties:confirm().
get_needs_confirming(#message_properties{needs_confirming = Value}) -> Value.

-spec get_size(message_properties()) -> message_properties:size().
get_size(#message_properties{size = Value}) -> Value.

-spec get_delivery_count(message_properties()) -> message_properties:delivery_count().
get_delivery_count(_MessageProps) -> 0.

-spec set_expiry(message_properties(), message_properties:expiry()) -> message_properties().
set_expiry(#message_properties{} = MessageProps, Expiry) ->
    MessageProps#message_properties{expiry = Expiry}.

-spec set_needs_confirming(message_properties(), message_properties:confirm()) ->
    message_properties().
set_needs_confirming(#message_properties{} = MessageProps, Confirm) ->
    MessageProps#message_properties{needs_confirming = Confirm}.

-spec set_delivery_count(message_properties(), message_properties:delivery_count()) ->
    message_properties().
set_delivery_count(#message_properties{} = MessageProps, _DeliveryCount) ->
    MessageProps.
