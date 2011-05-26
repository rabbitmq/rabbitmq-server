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

-module(rabbit_federation_util).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-export([purpose_arg/0, has_purpose_arg/1, federation_up/0, local_params/0,
         upstream_from_table/2]).

%%----------------------------------------------------------------------------

purpose_arg() -> {<<"x-purpose">>, longstr, <<"federation">>}.

has_purpose_arg(X) ->
    #exchange{arguments = Args} = rabbit_exchange:lookup_or_die(X),
    rabbit_misc:table_lookup(Args, <<"x-purpose">>) ==
        {longstr, <<"federation">>}.

federation_up() ->
    lists:keysearch(rabbitmq_federation, 1,
                    application:which_applications(infinity)) =/= false.

local_params() ->
    {ok, U} = application:get_env(rabbitmq_federation, local_username),
    #amqp_params_direct{username = list_to_binary(U)}.

upstream_from_table(Table, #resource{name = DX, virtual_host = DVHost}) ->
    TableProps = [{list_to_atom(binary_to_list(K)), V} || {K, _T, V} <- Table],
    {ok, Brokers} = application:get_env(rabbitmq_federation, brokers),
    UsableProps = [Merged ||
                      BrokerProps <- binaryise_all(Brokers),
                      Merged <- [merge(BrokerProps, TableProps)],
                      all_match(BrokerProps, Merged)],
    Props = case UsableProps of
                []    -> TableProps;
                [P|_] -> P
            end,
    upstream_from_properties(Props, DX, DVHost).

%%----------------------------------------------------------------------------

%% For all the props in Props1, does Props2 match?
all_match(Props1, Props2) ->
    lists:all(fun ({K, V}) ->
                      proplists:get_value(K, Props2) == V
              end, [KV || KV <- Props1]).

%% Add elements of Props1 which are not in Props2 - i.e. Props2 wins in event
%% of a clash
merge(Props1, Props2) ->
    lists:foldl(fun({K, V}, P) ->
                        case proplists:is_defined(K, Props2) of
                            true  -> P;
                            false -> [{K, V}|P]
                        end
                end, Props2, Props1).

binaryise_all(Items) -> [binaryise(I) || I <- Items].

binaryise(Props) -> [{K, binaryise0(V)} || {K, V} <- Props].

binaryise0(L) when is_list(L) -> list_to_binary(L);
binaryise0(X)                 -> X.

upstream_from_properties(P, DX, DVHost) ->
    #upstream{params          = amqp_params_from_properties(P, DVHost),
              exchange        = proplists:get_value(exchange,        P, DX),
              prefetch_count  = proplists:get_value(prefetch_count,  P, none),
              reconnect_delay = proplists:get_value(reconnect_delay, P, 1),
              queue_expires   = proplists:get_value(queue_expires,   P, 1800)}.

amqp_params_from_properties(P, DVHost) ->
    Params = #amqp_params_network{
      host         = binary_to_list(proplists:get_value(host, P)),
      port         = proplists:get_value(port,         P),
      virtual_host = proplists:get_value(virtual_host, P, DVHost),
      username     = proplists:get_value(username, P, <<"guest">>),
      password     = proplists:get_value(password, P, <<"guest">>)
     },
    lists:foldl(fun (F, ParamsIn) -> F(ParamsIn, P) end, Params,
                [fun set_ssl_options/2,
                 fun set_default_port/2,
                 fun set_heartbeat/2,
                 fun set_mechanisms/2]).

set_ssl_options(Params, Props) ->
    case proplists:get_value(protocol, Props, <<"amqp">>) of
        <<"amqp">>  -> Params;
        <<"amqps">> -> {ok, Opts} = application:get_env(
                                      rabbit_federation, ssl_options),
                       Params#amqp_params_network{ssl_options = Opts}
    end.

set_default_port(Params, _Props) ->
    case Params#amqp_params_network.port of
        undefined  -> Port = case Params#amqp_params_network.ssl_options of
                                 none -> 5672;
                                 _    -> 5671
                             end,
                      Params#amqp_params_network{port = Port};
        _          -> Params
    end.

set_heartbeat(Params, Props) ->
    case proplists:get_value(heartbeat, Props, none) of
        none -> Params;
        H    -> Params#amqp_params_network{heartbeat = H}
    end.

%% TODO it would be nice to support arbitrary mechanisms here.
set_mechanisms(Params, Props) ->
    case proplists:get_value(mechanism, Props, default) of
        default    -> Params;
        'EXTERNAL' -> Params#amqp_params_network{
                        auth_mechanisms =
                            [fun amqp_auth_mechanisms:external/3]};
        M          -> exit({unsupported_mechanism, M})
    end.
