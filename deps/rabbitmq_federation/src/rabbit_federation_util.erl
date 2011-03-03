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

-export([purpose_arg/0, has_purpose_arg/1, upstream_from_table/2,
         local_params/0]).

%%----------------------------------------------------------------------------

purpose_arg() ->
    {<<"x-purpose">>, longstr, <<"federation">>}.

has_purpose_arg(X) ->
    #exchange{arguments = Args} = rabbit_exchange:lookup_or_die(X),
    rabbit_misc:table_lookup(Args, <<"x-purpose">>) ==
        {longstr, <<"federation">>}.

%%----------------------------------------------------------------------------

local_params() ->
    {ok, U} = application:get_env(rabbit_federation, local_username),
    {ok, P} = application:get_env(rabbit_federation, local_password),
    #amqp_params{username = list_to_binary(U),
                 password = list_to_binary(P)}.

%%----------------------------------------------------------------------------

%% TODO this all badly needs a rewrite
upstream_from_table(Table, #resource{name = DX, virtual_host = DVHost}) ->
    Props = [{list_to_atom(binary_to_list(K)), V} || {K, _T, V} <- Table],
    AMQPParams = #amqp_params{
      host         = binary_to_list(proplists:get_value(host, Props)),
      port         = proplists:get_value(port,         Props),
      virtual_host = proplists:get_value(virtual_host, Props, DVHost)},
    {ok, Brokers} = application:get_env(rabbit_federation, brokers),
    Usable = [Merged || Broker <- Brokers,
                        Merged <- [merge(Broker, Props)],
                        all_match(Broker, Merged)],
    case Usable of
        []    -> P = upstream_from_broker([], Props, DX),
                 P#upstream{params = amqp_params_from_broker(AMQPParams, [])};
        [B|_] -> P = upstream_from_broker(B, Props, DX),
                 P#upstream{params = amqp_params_from_broker(AMQPParams, B)}
    end.

upstream_from_broker(B, Props, DX) ->
    #upstream{exchange        = proplists:get_value(exchange, Props,    DX),
              prefetch_count  = proplists:get_value(prefetch_count,  B, 100),
              reconnect_delay = proplists:get_value(reconnect_delay, B, 1),
              queue_expires   = proplists:get_value(queue_expires,   B, 1800)}.

amqp_params_from_broker(P, B) ->
    P1 = P#amqp_params{
           username = list_to_binary(proplists:get_value(username, B, "guest")),
           password = list_to_binary(proplists:get_value(password, B, "guest"))
          },
    P2 = case proplists:get_value(protocol, B, "amqp") of
             "amqp"  -> P1;
             "amqps" -> {ok, Opts} = application:get_env(
                                       rabbit_federation, ssl_options),
                        P1#amqp_params{ssl_options = Opts}
         end,
    P3 = case P2#amqp_params.port of
             undefined  -> Port = case P2#amqp_params.ssl_options of
                                      none -> 5672;
                                      _    -> 5671
                                  end,
                           P2#amqp_params{port = Port};
             _          -> P2
         end,
    P4 = case proplists:get_value(heartbeat, B, none) of
             none -> P3;
             H    -> P3#amqp_params{heartbeat = H}
         end,
    case proplists:get_value(mechanism, B, default) of
        default    -> P4;
        %% TODO it would be nice to support arbitrary mechanisms here.
        'EXTERNAL' -> P4#amqp_params{auth_mechanisms =
                                         [fun amqp_auth_mechanisms:external/3]};
        M          -> exit({unsupported_mechanism, M})
    end.

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

