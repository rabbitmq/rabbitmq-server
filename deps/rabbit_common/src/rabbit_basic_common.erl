%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_basic_common).
-include("rabbit.hrl").

-export([build_content/2, from_content/1]).

-spec build_content
        (rabbit_framing:amqp_property_record(), binary() | [binary()]) ->
            rabbit_types:content().
-spec from_content
        (rabbit_types:content()) ->
            {rabbit_framing:amqp_property_record(), binary()}.

build_content(Properties, BodyBin) when is_binary(BodyBin) ->
    build_content(Properties, [BodyBin]);

build_content(Properties, PFR) ->
    %% basic.publish hasn't changed so we can just hard-code amqp_0_9_1
    {ClassId, _MethodId} =
        rabbit_framing_amqp_0_9_1:method_id('basic.publish'),
    #content{class_id = ClassId,
             properties = Properties,
             properties_bin = none,
             protocol = none,
             payload_fragments_rev = PFR}.

from_content(Content) ->
    #content{class_id = ClassId,
             properties = Props,
             payload_fragments_rev = FragmentsRev} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    %% basic.publish hasn't changed so we can just hard-code amqp_0_9_1
    {ClassId, _MethodId} =
        rabbit_framing_amqp_0_9_1:method_id('basic.publish'),
    {Props, list_to_binary(lists:reverse(FragmentsRev))}.
