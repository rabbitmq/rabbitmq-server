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

-module(ssl_client_SUITE).

-export([test_coverage/0]).

-include("amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

basic_get_test() ->
    test_util:basic_get_test(new_connection()).

basic_get_ipv6_test() ->
    test_util:basic_get_test(new_connection(
                               #amqp_params_network{host = "::1"})).
basic_return_test() ->
    test_util:basic_return_test(new_connection()).

basic_qos_test() ->
    test_util:basic_qos_test(new_connection()).

basic_recover_test() -> 
    test_util:basic_recover_test(new_connection()).

basic_consume_test() -> 
    test_util:basic_consume_test(new_connection()).

lifecycle_test() ->
    test_util:lifecycle_test(new_connection()).

basic_ack_test() ->
    test_util:basic_ack_test(new_connection()).

channel_lifecycle_test() ->
    test_util:channel_lifecycle_test(new_connection()).

queue_unbind_test() ->
    test_util:queue_unbind_test(new_connection()).

teardown_test() ->
    test_util:teardown_test(new_connection()).

rpc_test() ->
    test_util:rpc_test(new_connection()).

%%---------------------------------------------------------------------------
%% Negative Tests

non_existent_exchange_test() -> 
    negative_test_util:non_existent_exchange_test(new_connection()).

hard_error_test() ->
    negative_test_util:hard_error_test(new_connection()).

%%---------------------------------------------------------------------------
%% Common Functions

new_connection() ->
    new_connection(#amqp_params_network{}).

new_connection(AmqpParams) ->
    {ok, [[CertsDir]]} = init:get_argument(erlang_client_ssl_dir),
    Params = AmqpParams#amqp_params_network
      {ssl_options = [{cacertfile, CertsDir ++ "/testca/cacert.pem"},
                      {certfile, CertsDir ++ "/client/cert.pem"},
                      {keyfile, CertsDir ++ "/client/key.pem"},
                      {verify, verify_peer},
                      {fail_if_no_peer_cert, true}]},
    case amqp_connection:start(Params) of
        {ok, Conn}         -> Conn;
        {error, _} = Error -> Error
    end.

test_coverage() ->
    rabbit_misc:enable_cover(),
    test(),
    rabbit_misc:report_cover().
