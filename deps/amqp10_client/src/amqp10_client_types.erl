-module(amqp10_client_types).

-include("rabbit_amqp1_0_framing.hrl").

-export([
         unpack/1,
         utf8/1,
         uint/1
        ]).

-type amqp10_performative() :: #'v1_0.open'{} | #'v1_0.begin'{} | #'v1_0.attach'{} |
                               #'v1_0.flow'{} | #'v1_0.transfer'{} |
                               #'v1_0.disposition'{} | #'v1_0.detach'{} |
                               #'v1_0.end'{} | #'v1_0.close'{}.

-type amqp10_msg_record() :: #'v1_0.transfer'{} | #'v1_0.header'{} |
                             #'v1_0.delivery_annotations'{} |
                             #'v1_0.message_annotations'{} |
                             #'v1_0.properties'{} |
                             #'v1_0.application_properties'{} |
                             #'v1_0.data'{} | #'v1_0.amqp_sequence'{} |
                             #'v1_0.amqp_value'{} | #'v1_0.footer'{}.

-type channel() :: non_neg_integer().

-type source() :: #'v1_0.source'{}.
-type target() :: #'v1_0.target'{}.

-export_type([amqp10_performative/0, channel/0,
              source/0, target/0, amqp10_msg_record/0
             ]).


unpack(undefined) -> undefined;
unpack({_, Value}) -> Value;
unpack(Value) -> Value.

utf8(S) when is_list(S) -> {utf8, list_to_binary(S)};
utf8(B) when is_binary(B) -> {utf8, B}.

uint(N) -> {uint, N}.
