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
-include("rabbit_federation.hrl").

-behaviour(rabbit_exchange_type).
-behaviour(gen_server2).

-export([description/0, route/2]).
-export([validate/1, create/2, recover/2, delete/3,
         add_binding/3, remove_bindings/3, assert_args_equivalence/2]).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([go/1]).

%%----------------------------------------------------------------------------

-define(TX, false).

%%----------------------------------------------------------------------------

description() ->
    [{name, <<"x-federation">>},
     {description, <<"Federation exchange">>}].

route(X, Delivery) ->
    with_module(X, fun (M) -> M:route(X, Delivery) end).

validate(X = #exchange{arguments = Args}) ->
    validate_arg(<<"upstreams">>, array,   Args),
    validate_arg(<<"type">>,      longstr, Args),
    {array, Upstreams} = rabbit_misc:table_lookup(Args, <<"upstreams">>),
    [validate_upstream(U) || U <- Upstreams],
    {longstr, TypeBin} = rabbit_misc:table_lookup(Args, <<"type">>),
    Type = rabbit_exchange:check_type(TypeBin),
    case Type of
        'x-federation' -> fail("Type argument must not be x-federation.", []);
        _              -> ok
    end,
    with_module(X, fun (M) -> M:validate(X) end).

create(?TX, X) ->
    {ok, _} = rabbit_federation_sup:start_child(exchange_to_sup_args(X)),
    with_module(X, fun (M) -> M:create(?TX, X) end);
create(Tx, X) ->
    with_module(X, fun (M) -> M:create(Tx, X) end).

recover(X, Bs) ->
    {ok, _} = rabbit_federation_sup:start_child(exchange_to_sup_args(X)),
    with_module(X, fun (M) -> M:recover(X, Bs) end).

delete(?TX, X, Bs) ->
    ok = rabbit_federation_sup:stop_child(exchange_to_sup_args(X)),
    with_module(X, fun (M) -> M:delete(?TX, X, Bs) end);
delete(Tx, X, Bs) ->
    with_module(X, fun (M) -> M:delete(Tx, X, Bs) end).

add_binding(?TX, X, B) ->
    call(X, {add_binding, B}),
    with_module(X, fun (M) -> M:add_binding(?TX, X, B) end);
add_binding(Tx, X, B) ->
    with_module(X, fun (M) -> M:add_binding(Tx, X, B) end).

remove_bindings(?TX, X, Bs) ->
    call(X, {remove_bindings, Bs}),
    with_module(X, fun (M) -> M:remove_bindings(?TX, X, Bs) end);
remove_bindings(Tx, X, Bs) ->
    with_module(X, fun (M) -> M:remove_bindings(Tx, X, Bs) end).

assert_args_equivalence(X = #exchange{name = Name, arguments = Args},
                        NewArgs) ->
    rabbit_misc:assert_args_equivalence(Args, NewArgs, Name,
                                        [<<"upstream">>, <<"type">>]),
    with_module(X, fun (M) -> M:assert_args_equivalence(X, Args) end).

%%----------------------------------------------------------------------------

go(Pid) ->
    gen_server2:call(Pid, become_real, infinity),
    gen_server2:cast(Pid, connect_all).

%%----------------------------------------------------------------------------

call(#exchange{ name = Downstream }, Msg) ->
    [{_, Pid}] = ets:lookup(?ETS_NAME, Downstream),
    gen_server2:call(Pid, Msg, infinity).

with_module(#exchange{ arguments = Args }, Fun) ->
    %% TODO should this be cached? It's on the publish path.
    {longstr, Type} = rabbit_misc:table_lookup(Args, <<"type">>),
    {ok, Module} = rabbit_registry:lookup_module(
                     exchange, rabbit_exchange:check_type(Type)),
    Fun(Module).

%%----------------------------------------------------------------------------

start_link(Args) ->
    gen_server2:start_link(?MODULE, Args, [{timeout, infinity}]).

%%----------------------------------------------------------------------------

init({DownstreamX, Durable, UpstreamURIs}) ->
    true = ets:insert(?ETS_NAME, {DownstreamX, self()}),
    {ok, #state{downstream_exchange = DownstreamX,
                downstream_durable = Durable,
                upstreams = UpstreamURIs}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(become_real, From, State) ->
    gen_server2:reply(From, ok),
    {become, rabbit_federation_exchange_process, State};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%%----------------------------------------------------------------------------

exchange_to_sup_args(#exchange{ name = Downstream, durable = Durable,
                                arguments = Args }) ->
    {array, UpstreamURIs0} =
        rabbit_misc:table_lookup(Args, <<"upstreams">>),
    UpstreamURIs = [U || {longstr, U} <- UpstreamURIs0],
    {Downstream, Durable, UpstreamURIs}.

validate_arg(Name, Type, Args) ->
    case rabbit_misc:table_lookup(Args, Name) of
        {Type, _} -> ok;
        undefined -> fail("Argument ~s missing", [Name]);
        _         -> fail("Argument ~s must be of type ~s", [Name, Type])
    end.

validate_upstream({longstr, URI}) ->
    case rabbit_federation_util:parse_uri(URI) of
        {error, E} ->
            fail("URI ~s could not be parsed, error: ~p", [URI, E]);
        Props ->
            case proplists:get_value(scheme, Props) of
                "amqp" -> ok;
                S      -> fail("Scheme ~s not supported", [S])
            end
    end;
validate_upstream({Type, URI}) ->
    fail("URI ~w was of type ~s, not longstr", [URI, Type]).

fail(Fmt, Args) ->
    rabbit_misc:protocol_error(precondition_failed, Fmt, Args).


