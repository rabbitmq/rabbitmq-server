%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_amqp_util).
-include("rabbit_amqp.hrl").

-export([protocol_error/3,
         capabilities/1,
         section_field_name_to_atom/1,
         jms_header_to_amqp_field_name/1
        ]).

-type field_name() :: durable | priority |
                      message_id | user_id | to | subject | reply_to |
                      correlation_id | content_type | content_encoding |
                      absolute_expiry_time | creation_time | group_id |
                      group_sequence | reply_to_group_id.

-export_type([field_name/0]).

-spec protocol_error(term(), io:format(), [term()]) ->
    no_return().
protocol_error(Condition, Msg, Args) ->
    Description = unicode:characters_to_binary(lists:flatten(io_lib:format(Msg, Args))),
    Reason = #'v1_0.error'{condition = Condition,
                           description = {utf8, Description}},
    exit(Reason).

-spec capabilities([binary()]) ->
    undefined | {array, symbol, [{symbol, binary()}]}.
capabilities([]) ->
    undefined;
capabilities(Capabilities) ->
    Caps = [{symbol, C} || C <- Capabilities],
    {array, symbol, Caps}.

-spec section_field_name_to_atom(binary()) -> field_name().
%% header section
section_field_name_to_atom(<<"durable">>) -> durable;
section_field_name_to_atom(<<"priority">>) -> priority;
%% ttl, first-acquirer, and delivery-count are unsupported
%% because setting a JMS message selector on these fields is invalid.

%% properties section
section_field_name_to_atom(<<"message-id">>) -> message_id;
section_field_name_to_atom(<<"user-id">>) -> user_id;
section_field_name_to_atom(<<"to">>) -> to;
section_field_name_to_atom(<<"subject">>) -> subject;
section_field_name_to_atom(<<"reply-to">>) -> reply_to;
section_field_name_to_atom(<<"correlation-id">>) -> correlation_id;
section_field_name_to_atom(<<"content-type">>) -> content_type;
section_field_name_to_atom(<<"content-encoding">>) -> content_encoding;
section_field_name_to_atom(<<"absolute-expiry-time">>) -> absolute_expiry_time;
section_field_name_to_atom(<<"creation-time">>) -> creation_time;
section_field_name_to_atom(<<"group-id">>) -> group_id;
section_field_name_to_atom(<<"group-sequence">>) -> group_sequence;
section_field_name_to_atom(<<"reply-to-group-id">>) -> reply_to_group_id;
section_field_name_to_atom(Other) -> throw({unsupported_field_name, Other}).

-spec jms_header_to_amqp_field_name(binary()) -> field_name() | binary().
%% "Message header field references are restricted to
%% JMSDeliveryMode, JMSPriority, JMSMessageID, JMSTimestamp, JMSCorrelationID, and JMSType."
%% https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax
%% amqp-bindmap-jms-v1.0-wd10 § 3.2.1 JMS Headers
jms_header_to_amqp_field_name(<<"JMSDeliveryMode">>) -> durable;
jms_header_to_amqp_field_name(<<"JMSPriority">>) -> priority;
jms_header_to_amqp_field_name(<<"JMSMessageID">>) -> message_id;
jms_header_to_amqp_field_name(<<"JMSTimestamp">>) -> creation_time;
jms_header_to_amqp_field_name(<<"JMSCorrelationID">>) -> correlation_id;
jms_header_to_amqp_field_name(<<"JMSType">>) -> subject;
%% amqp-bindmap-jms-v1.0-wd10 § 3.2.2 JMS-defined ’JMSX’ Properties
jms_header_to_amqp_field_name(<<"JMSXUserID">>) -> user_id;
jms_header_to_amqp_field_name(<<"JMSXGroupID">>) -> group_id;
jms_header_to_amqp_field_name(<<"JMSXGroupSeq">>) -> group_sequence;
jms_header_to_amqp_field_name(Other) -> Other.
