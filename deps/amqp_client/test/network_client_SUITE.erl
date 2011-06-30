%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(network_client_SUITE).

-export([test_coverage/0, new_connection/1]).

-include("amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

basic_get_test() ->
    test_util:basic_get_test(new_connection()).

basic_return_test() ->
    test_util:basic_return_test(new_connection()).

simultaneous_close_test_() ->
    test_util:repeat_eunit(
        fun () -> test_util:simultaneous_close_test(new_connection()) end).

basic_qos_test() ->
    test_util:basic_qos_test(new_connection()).

basic_recover_test() ->
    test_util:basic_recover_test(new_connection()).

basic_consume_test() ->
    test_util:basic_consume_test(new_connection()).

consume_notification_test() ->
    test_util:consume_notification_test(new_connection()).

basic_nack_test() ->
    test_util:basic_nack_test(new_connection()).

large_content_test() ->
    test_util:large_content_test(new_connection()).

lifecycle_test() ->
    test_util:lifecycle_test(new_connection()).

nowait_exchange_declare_test() ->
    test_util:nowait_exchange_declare_test(new_connection()).

channel_repeat_open_close_test() ->
    test_util:channel_repeat_open_close_test(new_connection()).

channel_multi_open_close_test() ->
    test_util:channel_multi_open_close_test(new_connection()).

basic_ack_test() ->
    test_util:basic_ack_test(new_connection()).

basic_ack_call_test() ->
    test_util:basic_ack_call_test(new_connection()).

channel_lifecycle_test() ->
    test_util:channel_lifecycle_test(new_connection()).

queue_unbind_test() ->
    test_util:queue_unbind_test(new_connection()).

sync_method_serialization_test_() ->
    {timeout, 60,
        fun () ->
                test_util:sync_method_serialization_test(new_connection())
        end}.

async_sync_method_serialization_test_() ->
    {timeout, 60,
        fun () ->
                test_util:async_sync_method_serialization_test(new_connection())
        end}.

sync_async_method_serialization_test_() ->
    {timeout, 60,
        fun () ->
                test_util:sync_async_method_serialization_test(new_connection())
        end}.

teardown_test_() ->
    test_util:repeat_eunit(
        fun () -> test_util:teardown_test(new_connection()) end).

rpc_test() ->
    test_util:rpc_test(new_connection()).

pub_and_close_test_() ->
    {timeout, 50,
        fun () ->
                test_util:pub_and_close_test(new_connection(), new_connection())
        end}.

channel_tune_negotiation_test() ->
    amqp_connection:close(
      new_connection(#amqp_params_network{channel_max = 10})).

confirm_test() ->
    test_util:confirm_test(new_connection()).

default_consumer_test() ->
    test_util:default_consumer_test(new_connection()).

subscribe_nowait_test() ->
    test_util:subscribe_nowait_test(new_connection()).

%%---------------------------------------------------------------------------
%% Negative Tests

non_existent_exchange_test() ->
    negative_test_util:non_existent_exchange_test(new_connection()).

bogus_rpc_test_() ->
    test_util:repeat_eunit(
        fun () -> negative_test_util:bogus_rpc_test(new_connection()) end).

hard_error_test_() ->
    test_util:repeat_eunit(
        fun () -> negative_test_util:hard_error_test(new_connection()) end).

non_existent_user_test() ->
    negative_test_util:non_existent_user_test(fun new_connection/3).

invalid_password_test() ->
    negative_test_util:invalid_password_test(fun new_connection/3).

non_existent_vhost_test() ->
    negative_test_util:non_existent_vhost_test(fun new_connection/3).

no_permission_test() ->
    negative_test_util:no_permission_test(fun new_connection/3).

channel_writer_death_test() ->
    negative_test_util:channel_writer_death_test(new_connection()).

channel_death_test() ->
    negative_test_util:channel_death_test(new_connection()).

shortstr_overflow_property_test() ->
    negative_test_util:shortstr_overflow_property_test(new_connection()).

shortstr_overflow_field_test() ->
    negative_test_util:shortstr_overflow_field_test(new_connection()).

command_invalid_over_channel_test() ->
    negative_test_util:command_invalid_over_channel_test(new_connection()).

command_invalid_over_channel0_test() ->
    negative_test_util:command_invalid_over_channel0_test(new_connection()).

%%---------------------------------------------------------------------------
%% Common Functions

new_connection() ->
    new_connection(#amqp_params_network{}).

new_connection(Username, Password, VHost) ->
    new_connection(#amqp_params_network{username     = Username,
                                        password     = Password,
                                        virtual_host = VHost}).

new_connection(AmqpParams) ->
    case amqp_connection:start(AmqpParams) of
        {ok, Conn}     -> Conn;
        {error, _} = E -> E
    end.

test_coverage() ->
    rabbit_misc:enable_cover(),
    test(),
    rabbit_misc:report_cover().
