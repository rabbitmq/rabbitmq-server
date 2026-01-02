%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
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

%% [Filter-Expressions-v1.0] § 6.4.4.4
%% https://docs.oasis-open.org/amqp/filtex/v1.0/csd01/filtex-v1.0-csd01.html#_Toc67929312
-spec section_field_name_to_atom(binary()) -> field_name() | binary() | error.
section_field_name_to_atom(<<"header.", FieldName/binary>>) ->
    header_field_name_to_atom(FieldName);
section_field_name_to_atom(<<"h.", FieldName/binary>>) ->
    header_field_name_to_atom(FieldName);
section_field_name_to_atom(<<"properties.", FieldName/binary>>) ->
    properties_field_name_to_atom(FieldName);
section_field_name_to_atom(<<"p.", FieldName/binary>>) ->
    properties_field_name_to_atom(FieldName);
section_field_name_to_atom(<<"application_properties.", FieldName/binary>>) ->
    FieldName;
section_field_name_to_atom(<<"a.", FieldName/binary>>) ->
    FieldName;
section_field_name_to_atom(_) ->
    error.

header_field_name_to_atom(<<"priority">>) -> priority;
header_field_name_to_atom(_) -> error.

properties_field_name_to_atom(<<"message_id">>) -> message_id;
properties_field_name_to_atom(<<"user_id">>) -> user_id;
properties_field_name_to_atom(<<"to">>) -> to;
properties_field_name_to_atom(<<"subject">>) -> subject;
properties_field_name_to_atom(<<"reply_to">>) -> reply_to;
properties_field_name_to_atom(<<"correlation_id">>) -> correlation_id;
properties_field_name_to_atom(<<"content_type">>) -> content_type;
properties_field_name_to_atom(<<"content_encoding">>) -> content_encoding;
properties_field_name_to_atom(<<"absolute_expiry_time">>) -> absolute_expiry_time;
properties_field_name_to_atom(<<"creation_time">>) -> creation_time;
properties_field_name_to_atom(<<"group_id">>) -> group_id;
properties_field_name_to_atom(<<"group_sequence">>) -> group_sequence;
properties_field_name_to_atom(<<"reply_to_group_id">>) -> reply_to_group_id;
properties_field_name_to_atom(_) -> error.

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
