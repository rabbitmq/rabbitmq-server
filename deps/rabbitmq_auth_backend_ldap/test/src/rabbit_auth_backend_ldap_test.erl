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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
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
     ?_test(succ(?SIMON))].

succ(Params) -> ?assertMatch({ok, _}, amqp_connection:start(Params)).
fail(Params) -> ?assertMatch({error, _}, amqp_connection:start(Params)).

resource_test_() ->
    X = #'exchange.declare'{exchange = <<"test">>},
    Q = #'queue.declare'{queue = <<"test">>},
    [fun() ->
             {ok, Conn} = amqp_connection:start(Person),
             {ok, Ch} = amqp_connection:open_channel(Conn),
             ?assertEqual(Result, try amqp_channel:call(Ch, Thing), ok
                                  catch exit:_ -> fail
                                  end)
     end || {Person, Thing, Result} <- [{?SIMON, X, ok},
                                        {?SIMON, Q, ok},
                                        {?MIKEB, X, fail},
                                        {?MIKEB, Q, ok}]].
