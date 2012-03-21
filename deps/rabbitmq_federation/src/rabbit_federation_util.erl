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

-export([local_params/1, pget_bin/2, pget_bin/3]).

-import(rabbit_misc, [pget_or_die/2, pget/3]).

%%----------------------------------------------------------------------------

local_params(VHost) ->
    U = rabbit_cluster_config:lookup(federation, local_username, <<"guest">>),
    #amqp_params_direct{username     = U,
                        virtual_host = VHost}.

pget_bin(K, T) -> list_to_binary(pget_or_die(K, T)).

pget_bin(K, T, D) when is_binary(D) -> pget_bin(K, T, binary_to_list(D));
pget_bin(K, T, D) when is_list(D)   -> list_to_binary(pget(K, T, D)).
