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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_queue_test).

-include("rabbit_federation.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_misc, [pget/2]).
-import(rabbit_federation_util, [name/1]).

-import(rabbit_federation_test_util, [publish_expect/5]).

-define(UPSTREAM_DOWNSTREAM, [q(<<"upstream">>),
                              q(<<"fed.downstream">>)]).

simple_test() ->
    with_ch(
      fun (Ch) ->
              publish_expect(Ch, <<>>, <<"upstream">>, <<"fed.downstream">>,
                             <<"HELLO">>)
      end, [q(<<"upstream">>),
            q(<<"fed.downstream">>)]).

%%----------------------------------------------------------------------------

with_ch(Fun, Qs) ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    declare_all(Ch, Qs),
    %%assert_status(Qs),
    Fun(Ch),
    delete_all(Ch, Qs),
    amqp_connection:close(Conn),
    ok.

declare_all(Ch, Qs) -> [declare_queue(Ch, Q) || Q <- Qs].
delete_all(Ch, Qs) ->
    [delete_queue(Ch, Q) || #'queue.declare'{queue = Q} <- Qs].

declare_queue(Ch, Q) ->
    amqp_channel:call(Ch, Q).

delete_queue(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

q(Name) ->
    #'queue.declare'{queue   = Name,
                     durable = true}.
