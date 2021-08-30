%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% TODO auto-generate

-module(rabbit_framing).

-export_type([protocol/0,
              amqp_field_type/0, amqp_property_type/0,
              amqp_table/0, amqp_array/0, amqp_value/0,
              amqp_method_name/0, amqp_method/0, amqp_method_record/0,
              amqp_method_field_name/0, amqp_property_record/0,
              amqp_exception/0, amqp_exception_code/0, amqp_class_id/0]).

-type protocol() :: 'rabbit_framing_amqp_0_8' | 'rabbit_framing_amqp_0_9_1'.

-define(protocol_type(T), type(T :: rabbit_framing_amqp_0_8:T |
                                    rabbit_framing_amqp_0_9_1:T)).

-?protocol_type(amqp_field_type()).
-?protocol_type(amqp_property_type()).
-?protocol_type(amqp_table()).
-?protocol_type(amqp_array()).
-?protocol_type(amqp_value()).
-?protocol_type(amqp_method_name()).
-?protocol_type(amqp_method()).
-?protocol_type(amqp_method_record()).
-?protocol_type(amqp_method_field_name()).
-?protocol_type(amqp_property_record()).
-?protocol_type(amqp_exception()).
-?protocol_type(amqp_exception_code()).
-?protocol_type(amqp_class_id()).
