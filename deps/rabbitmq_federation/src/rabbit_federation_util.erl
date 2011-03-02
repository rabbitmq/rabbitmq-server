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

-export([parse_uri/1, purpose_arg/0, has_purpose_arg/1, params_from_uri/1]).

parse_uri(URI) ->
    case uri_parser:parse(
           binary_to_list(URI), [{host, undefined}, {path, "/"},
                                 {port, 5672},      {'query', []}]) of
        {error, _} = E ->
            E;
        Props ->
            case string:tokens(proplists:get_value(path, Props), "/") of
                [VHostEnc, XEnc] ->
                    VHost = httpd_util:decode_hex(VHostEnc),
                    X = httpd_util:decode_hex(XEnc),
                    [{exchange, list_to_binary(X)},
                     {vhost,    list_to_binary(VHost)}] ++ Props;
                _ ->
                    {error, path_must_have_two_components}
            end
    end.

purpose_arg() ->
    {<<"x-purpose">>, longstr, <<"federation">>}.

has_purpose_arg(X) ->
    #exchange{arguments = Args} = rabbit_exchange:lookup_or_die(X),
    rabbit_misc:table_lookup(Args, <<"x-purpose">>) ==
        {longstr, <<"federation">>}.

%%----------------------------------------------------------------------------

params_from_uri(ExchangeURI) ->
    Props = rabbit_federation_util:parse_uri(ExchangeURI),
    AMQPParams = #amqp_params{host         = proplists:get_value(host, Props),
                              port         = proplists:get_value(port, Props),
                              virtual_host = proplists:get_value(vhost, Props)},
    {ok, Brokers} = application:get_env(rabbit_federation, brokers),
    Usable = [Merged || Broker <- Brokers,
                        Merged <- [merge(Broker, Props)],
                        all_match(Broker, Merged)],
    case Usable of
        []    -> P = params_from_broker([]),
                 P#params{connection = AMQPParams};
        [B|_] -> P = params_from_broker(B),
                 P#params{connection = amqp_params_from_broker(AMQPParams, B)}
    end.

params_from_broker(B) ->
    #params{prefetch_count  = proplists:get_value(prefetch_count,  B, 100),
            reconnect_delay = proplists:get_value(reconnect_delay, B, 1),
            queue_expires   = proplists:get_value(queue_expires,   B, 1800)}.

amqp_params_from_broker(P, B) ->
    P1 = P#amqp_params{
           username = list_to_binary(proplists:get_value(username, B, "guest")),
           password = list_to_binary(proplists:get_value(password, B, "guest"))
          },
    P2 = case proplists:get_value(scheme, B, "amqp") of
             "amqp"  -> P1;
             "amqps" -> {ok, Opts} = application:get_env(
                                       rabbit_federation, ssl_options),
                        P1#amqp_params{ssl_options = Opts,
                                       port        = 5671}
         end,
    P3 = case proplists:get_value(heartbeat, B, none) of
             none -> P2;
             H    -> P2#amqp_params{heartbeat = H}
         end,
    case proplists:get_value(mechanism, B, default) of
        default    -> P3;
        %% TODO it would be nice to support arbitrary mechanisms here.
        'EXTERNAL' -> P3#amqp_params{auth_mechanisms =
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

