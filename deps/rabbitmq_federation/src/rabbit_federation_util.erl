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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_util).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-export([local_params/1, should_forward/2, find_upstreams/2]).
-export([validate_arg/3, fail/2, vhost/1]).

-import(rabbit_misc, [pget_or_die/2, pget/3]).

%%----------------------------------------------------------------------------

local_params(VHost) ->
    U = rabbit_runtime_parameters:value(
          <<"federation">>, <<"local_username">>, <<"guest">>),
    #amqp_params_direct{username     = U,
                        virtual_host = VHost}.

should_forward(undefined, _MaxHops) ->
    true;
should_forward(Headers, MaxHops) ->
    case rabbit_misc:table_lookup(Headers, ?ROUTING_HEADER) of
        undefined  -> true;
        {array, A} -> length(A) < MaxHops
    end.

find_upstreams(ConnName, Upstreams) ->
    [U || U = #upstream{connection_name = ConnName2} <- Upstreams,
          ConnName =:= ConnName2].

validate_arg(Name, Type, Args) ->
    case rabbit_misc:table_lookup(Args, Name) of
        {Type, _} -> ok;
        undefined -> fail("Argument ~s missing", [Name]);
        _         -> fail("Argument ~s must be of type ~s", [Name, Type])
    end.

fail(Fmt, Args) -> rabbit_misc:protocol_error(precondition_failed, Fmt, Args).

vhost( #amqp_params_direct{virtual_host = VHost}) -> VHost;
vhost(#amqp_params_network{virtual_host = VHost}) -> VHost.
