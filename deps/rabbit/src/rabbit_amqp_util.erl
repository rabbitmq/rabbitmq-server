%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_amqp_util).
-include("rabbit_amqp.hrl").

-export([section_field_name_to_atom/1,
         capabilities/1,
         protocol_error/3]).

-type header_field_name() :: priority.
-type properties_field_name() :: message_id | user_id | to | subject | reply_to |
                                 correlation_id | content_type | content_encoding |
                                 absolute_expiry_time | creation_time | group_id |
                                 group_sequence | reply_to_group_id.
-type field_name() :: header_field_name() | properties_field_name().
-export_type([field_name/0]).

-spec section_field_name_to_atom(binary()) -> field_name() | binary().
section_field_name_to_atom(<<"header.priority">>) -> priority;
%% ttl, first-acquirer, and delivery-count are unsupported
%% because setting a JMS message selector on these fields is invalid.
section_field_name_to_atom(<<"header.", _/binary>> = Bin) -> throw({unsupported_field, Bin});
section_field_name_to_atom(<<"properties.message-id">>) -> message_id;
section_field_name_to_atom(<<"properties.user-id">>) -> user_id;
section_field_name_to_atom(<<"properties.to">>) -> to;
section_field_name_to_atom(<<"properties.subject">>) -> subject;
section_field_name_to_atom(<<"properties.reply-to">>) -> reply_to;
section_field_name_to_atom(<<"properties.correlation-id">>) -> correlation_id;
section_field_name_to_atom(<<"properties.content-type">>) -> content_type;
section_field_name_to_atom(<<"properties.content-encoding">>) -> content_encoding;
section_field_name_to_atom(<<"properties.absolute-expiry-time">>) -> absolute_expiry_time;
section_field_name_to_atom(<<"properties.creation-time">>) -> creation_time;
section_field_name_to_atom(<<"properties.group-id">>) -> group_id;
section_field_name_to_atom(<<"properties.group-sequence">>) -> group_sequence;
section_field_name_to_atom(<<"properties.reply-to-group-id">>) -> reply_to_group_id;
section_field_name_to_atom(<<"properties.", _/binary>> = Bin) -> throw({unsupported_field, Bin});
section_field_name_to_atom(Other) -> Other.

-spec capabilities([binary()]) ->
    undefined | {array, symbol, [{symbol, binary()}]}.
capabilities([]) ->
    undefined;
capabilities(Capabilities) ->
    Caps = [{symbol, C} || C <- Capabilities],
    {array, symbol, Caps}.

-spec protocol_error(term(), io:format(), [term()]) ->
    no_return().
protocol_error(Condition, Msg, Args) ->
    Description = unicode:characters_to_binary(lists:flatten(io_lib:format(Msg, Args))),
    Reason = #'v1_0.error'{condition = Condition,
                           description = {utf8, Description}},
    exit(Reason).
