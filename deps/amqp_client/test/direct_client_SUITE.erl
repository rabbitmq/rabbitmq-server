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

-module(direct_client_SUITE).

-define(RPC_TIMEOUT, 10000).
-define(RPC_SLEEP, 500).

-export([test_coverage/0]).

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

queue_unbind_test() ->
    test_util:queue_unbind_test(new_connection()).

rpc_test() ->
    test_util:rpc_test(new_connection()).

confirm_test() ->
    test_util:confirm_test(new_connection()).

default_consumer_test() ->
    test_util:default_consumer_test(new_connection()).

subscribe_nowait_test() ->
    test_util:subscribe_nowait_test(new_connection()).

%%---------------------------------------------------------------------------
%% Negative Tests
%%---------------------------------------------------------------------------

non_existent_exchange_test() ->
    negative_test_util:non_existent_exchange_test(new_connection()).

hard_error_test() ->
    negative_test_util:hard_error_test(new_connection()).

bogus_rpc_test() ->
    negative_test_util:bogus_rpc_test(new_connection()).

channel_death_test() ->
    negative_test_util:channel_death_test(new_connection()).

non_existent_user_test() ->
    negative_test_util:non_existent_user_test(fun new_connection/3).

non_existent_vhost_test() ->
    negative_test_util:non_existent_vhost_test(fun new_connection/3).

no_permission_test() ->
    negative_test_util:no_permission_test(fun new_connection/3).

command_invalid_over_channel_test() ->
    negative_test_util:command_invalid_over_channel_test(new_connection()).

%%---------------------------------------------------------------------------
%% Common Functions
%%---------------------------------------------------------------------------

new_connection() ->
    new_connection(#amqp_params_direct{}).

new_connection(Username, _Password, VHost) ->
    new_connection(#amqp_params_direct{username     = Username,
                                       virtual_host = VHost}).

new_connection(AmqpParams) ->
    Node = rabbit_misc:makenode(rabbit),
    case amqp_connection:start(AmqpParams#amqp_params_direct{node = Node}) of
        {ok, Conn}     -> Conn;
        {error, _} = E -> E
    end.

test_coverage() ->
    rabbit_misc:enable_cover(),
    test(),
    rabbit_misc:report_cover().
