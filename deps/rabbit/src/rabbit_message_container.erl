%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_message_container).

-record(message_container, {protocol, data, parsed, annotations}).

-type(message_container() ::
        #message_container{protocol :: atom(),
                           data :: any(),
                           parsed :: boolean(),
                           annotations :: map()}).

-export([new/3, new/4, get_data/1, get_internal/2, set_internal/3,
         prepare_to_store/1, serialize/1, msg_size/1]).

-spec new(atom(), any(), boolean()) -> message_container().
new(Protocol, Data, Parsed) ->
    new(Protocol, Data, Parsed, #{}).

new(Protocol, Data, Parsed, Annotations) ->
    #message_container{protocol = Protocol,
                       data = Data,
                       parsed = Parsed,
                       annotations = Annotations}.

get_data(#message_container{data = Data}) ->
    Data.

get_internal(#message_container{protocol = Protocol, data = Data}, Key) ->
    Mod = get_module(Protocol),
    Mod:get_internal(Data, Key).

set_internal(#message_container{protocol = Protocol, data = Data} = MessageContainer, Key, Value) ->
    %% Only used by DLX
    Mod = get_module(Protocol),
    MessageContainer#message_container{data = Mod:set_internal(Data, Key, Value)}.

get_module(amqp091) ->
    rabbit_message_container_091.

prepare_to_store(#message_container{protocol = Protocol, data = Data} = MessageContainer) ->
    %% We might have to strip down the container even more fore storage
    Mod = get_module(Protocol),
    MessageContainer#message_container{data = Mod:prepare_to_store(Data)}.

serialize(#message_container{protocol = Protocol, data = Data}) ->
    Mod = get_module(Protocol),
    Mod:serialize(Data).

msg_size(#message_container{protocol = Protocol, data = Data}) ->
    Mod = get_module(Protocol),
    Mod:msg_size(Data).
