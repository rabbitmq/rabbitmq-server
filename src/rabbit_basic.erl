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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_basic).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-export([publish/1, message/4, properties/1, delivery/5]).
-export([publish/4, publish/7]).
-export([build_content/2, from_content/1]).
-export([is_message_persistent/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(properties_input() ::
        (rabbit_framing:amqp_property_record() | [{atom(), any()}])).
-type(publish_result() ::
        ({ok, rabbit_router:routing_result(), [pid()]}
         | rabbit_types:error('not_found'))).

-spec(publish/1 ::
        (rabbit_types:delivery()) -> publish_result()).
-spec(delivery/5 ::
        (boolean(), boolean(), rabbit_types:maybe(rabbit_types:txn()),
         rabbit_types:message(), undefined | integer()) ->
                         rabbit_types:delivery()).
-spec(message/4 ::
        (rabbit_exchange:name(), rabbit_router:routing_key(),
         properties_input(), binary()) ->
                        (rabbit_types:message() | rabbit_types:error(any()))).
-spec(properties/1 ::
        (properties_input()) -> rabbit_framing:amqp_property_record()).
-spec(publish/4 ::
        (rabbit_exchange:name(), rabbit_router:routing_key(),
         properties_input(), binary()) -> publish_result()).
-spec(publish/7 ::
        (rabbit_exchange:name(), rabbit_router:routing_key(),
         boolean(), boolean(), rabbit_types:maybe(rabbit_types:txn()),
         properties_input(), binary()) -> publish_result()).
-spec(build_content/2 :: (rabbit_framing:amqp_property_record(), binary()) ->
                              rabbit_types:content()).
-spec(from_content/1 :: (rabbit_types:content()) ->
                             {rabbit_framing:amqp_property_record(), binary()}).
-spec(is_message_persistent/1 :: (rabbit_types:decoded_content()) ->
                                      (boolean() |
                                       {'invalid', non_neg_integer()})).

-endif.

%%----------------------------------------------------------------------------

publish(Delivery = #delivery{
          message = #basic_message{exchange_name = ExchangeName}}) ->
    case rabbit_exchange:lookup(ExchangeName) of
        {ok, X} ->
            {RoutingRes, DeliveredQPids} = rabbit_exchange:publish(X, Delivery),
            {ok, RoutingRes, DeliveredQPids};
        Other ->
            Other
    end.

delivery(Mandatory, Immediate, Txn, Message, MsgSeqNo) ->
    #delivery{mandatory = Mandatory, immediate = Immediate, txn = Txn,
              sender = self(), message = Message, msg_seq_no = MsgSeqNo}.

build_content(Properties, BodyBin) ->
    %% basic.publish hasn't changed so we can just hard-code amqp_0_9_1
    {ClassId, _MethodId} =
        rabbit_framing_amqp_0_9_1:method_id('basic.publish'),
    #content{class_id = ClassId,
             properties = Properties,
             properties_bin = none,
             protocol = none,
             payload_fragments_rev = [BodyBin]}.

from_content(Content) ->
    #content{class_id = ClassId,
             properties = Props,
             payload_fragments_rev = FragmentsRev} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    %% basic.publish hasn't changed so we can just hard-code amqp_0_9_1
    {ClassId, _MethodId} =
        rabbit_framing_amqp_0_9_1:method_id('basic.publish'),
    {Props, list_to_binary(lists:reverse(FragmentsRev))}.

message(ExchangeName, RoutingKeyBin, RawProperties, BodyBin) ->
    Properties = properties(RawProperties),
    Content = build_content(Properties, BodyBin),
    case is_message_persistent(Content) of
        {invalid, Other} ->
            {error, {invalid_delivery_mode, Other}};
        IsPersistent when is_boolean(IsPersistent) ->
            #basic_message{exchange_name  = ExchangeName,
                           routing_key    = RoutingKeyBin,
                           content        = Content,
                           guid           = rabbit_guid:guid(),
                           is_persistent  = IsPersistent}
    end.

properties(P = #'P_basic'{}) ->
    P;
properties(P) when is_list(P) ->
    %% Yes, this is O(length(P) * record_info(size, 'P_basic') / 2),
    %% i.e. slow. Use the definition of 'P_basic' directly if
    %% possible!
    lists:foldl(fun ({Key, Value}, Acc) ->
                        case indexof(record_info(fields, 'P_basic'), Key) of
                            0 -> throw({unknown_basic_property, Key});
                            N -> setelement(N + 1, Acc, Value)
                        end
                end, #'P_basic'{}, P).

indexof(L, Element) -> indexof(L, Element, 1).

indexof([], _Element, _N)              -> 0;
indexof([Element | _Rest], Element, N) -> N;
indexof([_ | Rest], Element, N)        -> indexof(Rest, Element, N + 1).

%% Convenience function, for avoiding round-trips in calls across the
%% erlang distributed network.
publish(ExchangeName, RoutingKeyBin, Properties, BodyBin) ->
    publish(ExchangeName, RoutingKeyBin, false, false, none, Properties,
            BodyBin).

%% Convenience function, for avoiding round-trips in calls across the
%% erlang distributed network.
publish(ExchangeName, RoutingKeyBin, Mandatory, Immediate, Txn, Properties,
        BodyBin) ->
    publish(delivery(Mandatory, Immediate, Txn,
                     message(ExchangeName, RoutingKeyBin,
                             properties(Properties), BodyBin),
                     undefined)).

is_message_persistent(#content{properties = #'P_basic'{
                                 delivery_mode = Mode}}) ->
    case Mode of
        1         -> false;
        2         -> true;
        undefined -> false;
        Other     -> {invalid, Other}
    end.
