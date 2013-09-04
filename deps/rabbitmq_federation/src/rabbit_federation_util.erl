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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_federation_util).

-include_lib("kernel/include/inet.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-export([local_nodename/1, should_forward/2, find_upstreams/2]).
-export([validate_arg/3, fail/2, name/1, vhost/1, r/1]).

-import(rabbit_misc, [pget_or_die/2, pget/3]).

%%----------------------------------------------------------------------------

local_nodename(VHost) ->
    rabbit_runtime_parameters:value(
      VHost, <<"federation">>, <<"local-nodename">>, local_nodename_implicit()).

local_nodename_implicit() ->
    {ID, _} = rabbit_nodes:parts(node()),
    {ok, Host} = inet:gethostname(),
    {ok, #hostent{h_name = FQDN}} = inet:gethostbyname(Host),
    list_to_binary(atom_to_list(rabbit_nodes:make({ID, FQDN}))).

should_forward(undefined, _MaxHops) ->
    true;
should_forward(Headers, MaxHops) ->
    case rabbit_misc:table_lookup(Headers, ?ROUTING_HEADER) of
        {array, A} -> length(A) < MaxHops;
        _          -> true
    end.

find_upstreams(Name, Upstreams) ->
    [U || U = #upstream{name = Name2} <- Upstreams,
          Name =:= Name2].

validate_arg(Name, Type, Args) ->
    case rabbit_misc:table_lookup(Args, Name) of
        {Type, _} -> ok;
        undefined -> fail("Argument ~s missing", [Name]);
        _         -> fail("Argument ~s must be of type ~s", [Name, Type])
    end.

fail(Fmt, Args) -> rabbit_misc:protocol_error(precondition_failed, Fmt, Args).

name(                 #resource{name = XName})  -> XName;
name(#exchange{name = #resource{name = XName}}) -> XName;
name(#amqqueue{name = #resource{name = QName}}) -> QName.

vhost(                 #resource{virtual_host = VHost})  -> VHost;
vhost(#exchange{name = #resource{virtual_host = VHost}}) -> VHost;
vhost(#amqqueue{name = #resource{virtual_host = VHost}}) -> VHost;
vhost( #amqp_params_direct{virtual_host = VHost}) -> VHost;
vhost(#amqp_params_network{virtual_host = VHost}) -> VHost.

r(#exchange{name = XName}) -> XName;
r(#amqqueue{name = QName}) -> QName.
