%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_framing).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").


-export([encode_method_fields/2]).
-export([decode_method_fields/3]).
-export([lookup_method_name/2]).
-export([method_id/2]).

-export([method_has_content/1]).
-export([is_method_synchronous/1]).
-export([method_record/1]).
-export([method_fieldnames/1]).
-export([decode_properties/2]).
-export([encode_properties/1]).
-export([lookup_amqp_exception/1]).
-export([amqp_exception/1]).

%% Method signatures
-ifdef(use_specs).
-spec(encode_method_fields/2 :: (amqp_method_record(), protocol()) -> binary()).
-spec(decode_method_fields/3 :: (amqp_method_name(), binary(), protocol()) -> 
                                     amqp_method_record()).
-spec(lookup_method_name/2 :: (amqp_method(), protocol()) -> 
                                   amqp_method_name()).
-spec(method_id/2 :: (amqp_method_name(), protocol()) -> amqp_method()).

-spec(method_has_content/1 :: (amqp_method_name()) -> boolean()).
-spec(is_method_synchronous/1 :: (amqp_method_record()) -> boolean()).
-spec(method_record/1 :: (amqp_method_name()) -> amqp_method_record()).
-spec(method_fieldnames/1 :: (amqp_method_name()) ->
                                  [amqp_method_field_name()]).
-spec(decode_properties/2 :: (non_neg_integer(), binary()) ->
                                  amqp_property_record()).
-spec(encode_properties/1 :: (amqp_method_record()) -> binary()).
-spec(lookup_amqp_exception/1 :: 
        (amqp_exception()) -> {boolean(), amqp_exception_code(), binary()}).
-spec(amqp_exception/1 :: (amqp_exception_code()) -> amqp_exception()).
-endif. % use_specs

encode_method_fields(MethodRecord, amqp_0_9_1) ->
    rabbit_framing_amqp_0_9_1:encode_method_fields(MethodRecord);
encode_method_fields(MethodRecord, amqp_0_8) ->
    rabbit_framing_amqp_0_8:encode_method_fields(MethodRecord).

decode_method_fields(MethodName, FieldsBin, amqp_0_9_1) ->
    rabbit_framing_amqp_0_9_1:decode_method_fields(MethodName, FieldsBin);
decode_method_fields(MethodName, FieldsBin, amqp_0_8) ->
    rabbit_framing_amqp_0_8:decode_method_fields(MethodName, FieldsBin).

lookup_method_name(ClassMethod, amqp_0_9_1) ->
    rabbit_framing_amqp_0_9_1:lookup_method_name(ClassMethod);
lookup_method_name(ClassMethod, amqp_0_8) ->
    rabbit_framing_amqp_0_8:lookup_method_name(ClassMethod).

method_id(MethodName, amqp_0_9_1) ->
    rabbit_framing_amqp_0_9_1:method_id(MethodName);
method_id(MethodName, amqp_0_8) ->
    rabbit_framing_amqp_0_8:method_id(MethodName).



%% These ones don't make any difference, let's just use 0-9-1.
method_has_content(X)    -> rabbit_framing_amqp_0_9_1:method_has_content(X).
method_record(X)         -> rabbit_framing_amqp_0_9_1:method_record(X).
method_fieldnames(X)     -> rabbit_framing_amqp_0_9_1:method_fieldnames(X).
encode_properties(X)     -> rabbit_framing_amqp_0_9_1:encode_properties(X).
amqp_exception(X)        -> rabbit_framing_amqp_0_9_1:amqp_exception(X).
lookup_amqp_exception(X) -> rabbit_framing_amqp_0_9_1:lookup_amqp_exception(X).
is_method_synchronous(X) -> rabbit_framing_amqp_0_9_1:is_method_synchronous(X).
decode_properties(X, Y)  -> rabbit_framing_amqp_0_9_1:decode_properties(X, Y).
