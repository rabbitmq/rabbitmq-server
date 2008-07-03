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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

%% TODO: much of this should be generated

-type(amqp_field_type() ::
      'longstr' | 'signedint' | 'decimal' | 'timestamp' |
      'table' | 'byte' | 'double' | 'float' | 'long' |
      'short' | 'bool' | 'binary' | 'void').
-type(amqp_property_type() ::
      'shortstr' | 'longstr' | 'octet' | 'shortint' | 'longint' |
      'longlongint' | 'timestamp' | 'bit' | 'table').
%% we could make this more precise but ultimately are limited by
%% dialyzer's lack of support for recursive types
-type(amqp_table() :: [{binary(), amqp_field_type(), any()}]).
%% TODO: make this more precise
-type(amqp_class_id() :: non_neg_integer()).
%% TODO: make this more precise
-type(amqp_properties() :: tuple()).
%% TODO: make this more precise
-type(amqp_method() :: tuple()).
%% TODO: make this more precise
-type(amqp_method_name() :: atom()).
-type(channel_number() :: non_neg_integer()).
%% TODO: make this more precise
-type(amqp_error() :: {bool(), non_neg_integer(), binary()}).
-type(name() :: binary()).
-type(routing_key() :: binary()).
-type(username() :: binary()).
-type(password() :: binary()).
-type(vhost() :: binary()).
-type(ctag() :: binary()).
-type(exchange_type() :: 'direct' | 'topic' | 'fanout').
