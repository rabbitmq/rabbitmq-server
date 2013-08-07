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
%% The Original Code is RabbitMQ
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_auth_backend_ldap_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(SIMON, #amqp_params_network{username     = <<"Simon MacMullen">>,
                                    password     = <<"password">>,
                                    virtual_host = <<"test">>}).

-define(MIKEB, #amqp_params_network{username     = <<"Mike Bridgen">>,
                                    password     = <<"password">>,
                                    virtual_host = <<"test">>}).

login_test_() ->
    [?_test(fail(#amqp_params_network{})),
     ?_test(fail(#amqp_params_network{username     = <<"Simon MacMullen">>})),
     ?_test(fail(#amqp_params_network{username     = <<"Simon MacMullen">>,
                                      password     = <<"password">>})),
     ?_test(succ(?SIMON)),
     ?_test(succ(?MIKEB))].

succ(Params) -> ?assertMatch({ok, _}, amqp_connection:start(Params)).
fail(Params) -> ?assertMatch({error, _}, amqp_connection:start(Params)).

in_group_test_() ->
    X = [#'exchange.declare'{exchange = <<"test">>}],
    [test_resource_fun(PTR) || PTR <- [{?SIMON, X, ok},
                                       {?MIKEB, X, fail}]].

const_test_() ->
    Q = [#'queue.declare'{queue = <<"test">>}],
    [test_resource_fun(PTR) || PTR <- [{?SIMON, Q, ok},
                                       {?MIKEB, Q, fail}]].

string_match_test_() ->
    B = fun(N) ->
                [#'exchange.declare'{exchange = N},
                 #'queue.declare'{queue = <<"test">>},
                 #'queue.bind'{exchange = N, queue = <<"test">>}]
        end,
    [test_resource_fun(PTR) ||
        PTR <- [{?SIMON, B(<<"xch-Simon MacMullen-abc123">>), ok},
                {?SIMON, B(<<"abc123">>),                     fail},
                {?SIMON, B(<<"xch-Someone Else-abc123">>),    fail}]].

boolean_logic_test_() ->
    Q1 = [#'queue.declare'{queue = <<"test1">>},
          #'basic.consume'{queue = <<"test1">>}],
    Q2 = [#'queue.declare'{queue = <<"test2">>},
          #'basic.consume'{queue = <<"test2">>}],
    [test_resource_fun(PTR) || PTR <- [{?SIMON, Q1, ok},
                                       {?SIMON, Q2, ok},
                                       {?MIKEB, Q1, fail},
                                       {?MIKEB, Q2, fail}]].

test_resource_fun({Person, Things, Result}) ->
    fun() ->
            {ok, Conn} = amqp_connection:start(Person),
            {ok, Ch} = amqp_connection:open_channel(Conn),
            ?assertEqual(Result,
                         try
                             [amqp_channel:call(Ch, T) || T <- Things],
                             amqp_connection:close(Conn),
                             ok
                         catch exit:_ -> fail
                         end)
    end.
