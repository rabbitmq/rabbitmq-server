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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_exchange).

-rabbit_boot_step({?MODULE,
                   [{description, "federation exchange type"},
                    {mfa, {rabbit_registry, register,
                           [exchange, <<"x-federation">>,
                            rabbit_federation_exchange]}},
                    {requires, rabbit_registry},
                    {enables, exchange_recovery}]}).

-include_lib("rabbit_common/include/rabbit_exchange_type_spec.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(rabbit_exchange_type).
-behaviour(gen_server).

-export([start/0]).

-export([description/0, route/2]).
-export([validate/1, create/2, recover/2, delete/3,
         add_binding/3, remove_bindings/3, assert_args_equivalence/2]).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

-define(ETS_NAME, ?MODULE).
-define(TX, false).
-record(state, { local_connection, local_channel,
                 remote_connection, remote_channel,
                 local, remote, remote_queue }).

%%----------------------------------------------------------------------------

start() ->
    ?ETS_NAME = ets:new(?ETS_NAME, [public, set, named_table]).

description() ->
    [{name, <<"x-federation">>},
     {description, <<"Federation exchange">>}].

route(X, Delivery) ->
    with_module(X, fun (M) -> M:route(X, Delivery) end).

validate(X) ->
    ok.
    %%with_module(X, fun (M) -> M:validate(X) end).

create(?TX, X = #exchange{ name = Local, arguments = Args }) ->
    {longstr, Remote} = rabbit_misc:table_lookup(Args, <<"remote">>),
    {longstr, Type} = rabbit_misc:table_lookup(Args, <<"type">>),
    {ok, Module} = rabbit_registry:lookup_module(
                     exchange, rabbit_exchange:check_type(Type)),
    rabbit_federation_sup:start_child(Local, binary_to_list(Remote), Module),
    with_module(X, fun (M) -> M:create(?TX, X) end);
create(Tx, X) ->
    ok.
    %%with_module(X, fun (M) -> M:create(Tx, X) end).

recover(X, Bs) ->
    with_module(X, fun (M) -> M:recover(X, Bs) end).

delete(Tx, X, Bs) ->
    with_module(X, fun (M) -> M:delete(Tx, X, Bs) end).

add_binding(?TX, X, B) ->
    call(X, {add_binding, B}),
    with_module(X, fun (M) -> M:add_binding(?TX, X, B) end);
add_binding(Tx, X, B) ->
    with_module(X, fun (M) -> M:add_binding(Tx, X, B) end).

remove_bindings(Tx, X, Bs) ->
    with_module(X, fun (M) -> M:remove_bindings(Tx, X, Bs) end).

assert_args_equivalence(X, Args) ->
    with_module(X, fun (M) -> M:assert_args_equivalence(X, Args) end).

%%----------------------------------------------------------------------------

call(#exchange{ name = Local }, Msg) ->
    [{_, Pid, _}] = ets:lookup(?ETS_NAME, Local),
    gen_server:call(Pid, Msg, infinity).

with_module(#exchange{ name = Local }, Fun) ->
    [{_, _, Module}] = ets:lookup(?ETS_NAME, Local),
    Fun(Module).

%%----------------------------------------------------------------------------

start_link(Local, Remote, Module) ->
    gen_server:start_link(?MODULE, {Local, Remote, Module},
                          [{timeout, infinity}]).

%%----------------------------------------------------------------------------

init({Local, RemoteURI, Module}) ->
    Remote0 = uri_parser:parse(RemoteURI, [{host, undefined}, {path, "/"},
                                           {port, undefined}, {'query', []}]),
    [VHostEnc, XEnc] = string:tokens(proplists:get_value(path, Remote0), "/"),
    VHost = httpd_util:decode_hex(VHostEnc),
    X = httpd_util:decode_hex(XEnc),
    Remote = [{vhost, VHost}, {exchange, X}] ++ Remote0,
    Params = #amqp_params{host = proplists:get_value(host, Remote),
                          virtual_host = list_to_binary(VHost)},
    {ok, RConn} = amqp_connection:start(network, Params),
    {ok, RCh} = amqp_connection:open_channel(RConn),
    #'queue.declare_ok' {queue = Q} =
        amqp_channel:call(RCh, #'queue.declare'{ exclusive = true}),
    amqp_channel:subscribe(RCh, #'basic.consume'{ queue = Q,
                                                  no_ack = true }, %% FIXME
                           self()),
    {ok, LConn} = amqp_connection:start(direct),
    {ok, LCh} = amqp_connection:open_channel(LConn),
    true = ets:insert(?ETS_NAME, {Local, self(), Module}),
    {ok, #state{local_connection = LConn, local_channel = LCh,
                remote_connection = RConn, remote_channel = RCh,
                local = Local,
                remote = Remote, remote_queue = Q} }.

handle_call({add_binding, #binding{key = Key, args = Args} }, _From,
            State = #state{ remote_channel = RCh,
                            remote = Remote, remote_queue = Q}) ->
    amqp_channel:call(RCh, #'queue.bind'{
                        queue = Q,
                        exchange = list_to_binary(
                                     proplists:get_value(exchange, Remote)),
                        routing_key = Key,
                        arguments = Args}),
    {reply, ok, State};

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info({#'basic.deliver'{delivery_tag = _DTag,
                              %%redelivered = Redelivered,
                              %%exchange = Exchange,
                              routing_key = Key},
             Msg}, State = #state{local = #resource {name = X},
                                  local_channel = LCh}) ->
    amqp_channel:cast(LCh, #'basic.publish'{exchange = X,
                                            routing_key = Key}, Msg),
    {noreply, State};

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State = #state { local_connection = LConn,
                                    remote_connection = RConn,
                                    local = Local }) ->
    amqp_connection:close(LConn),
    amqp_connection:close(RConn),
    true = ets:delete(?ETS_NAME, Local),
    State.
