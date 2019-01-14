%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
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
