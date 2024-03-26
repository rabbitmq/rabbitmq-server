%% This module is based on the autocluster_backend module
%% from rabbitmq-autocluster by Gavin Roy.
%%
%% Copyright (c) 2014-2015 AWeber Communications
%% Copyright (c) 2007-2024 Broadcom. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without modification,
%% are permitted provided that the following conditions are met:
%%
%%  * Redistributions of source code must retain the above copyright notice, this
%%    list of conditions and the following disclaimer.
%%  * Redistributions in binary form must reproduce the above copyright notice,
%%    this list of conditions and the following disclaimer in the documentation
%%    and/or other materials provided with the distribution.
%%  * Neither the name of the project nor the names of its
%%    contributors may be used to endorse or promote products derived from this
%%    software without specific prior written permission.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
%% ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
%% WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
%% IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
%% INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
%% BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
%% DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
%% LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
%% OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
%% ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
%%
%% The Original Code is rabbitmq-autocluster.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2014-2015 AWeber Communications
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_peer_discovery_backend).

-include("rabbit.hrl").

-callback init() -> ok | {error, Reason :: string()}.

-callback list_nodes() -> {ok, {Nodes :: list(), NodeType :: rabbit_types:node_type()}} |
                          {error, Reason :: string()}.

-callback supports_registration() -> boolean().

-callback register()   -> ok | {error, Reason :: string()}.

-callback unregister() -> ok | {error, Reason :: string()}.

-callback post_registration()   -> ok | {error, Reason :: string()}.

-callback pre_discovery() ->
    {ok, BackendPriv :: backend_priv()} | {error, Reason :: string()}.

-callback post_discovery(BackendPriv :: backend_priv()) ->
    ok.

-callback lock(Nodes :: [node()]) ->
    {ok, LockData :: lock_data()} |
    not_supported |
    {error, Reason :: string()}.

-callback lock(Nodes :: [node()], BackendPriv :: term()) ->
    {ok, LockData :: lock_data()} |
    not_supported |
    {error, Reason :: string()}.

-callback unlock(LockData :: lock_data()) ->
    ok.

-callback unlock(LockData :: lock_data(), BackendPriv :: backend_priv()) ->
    ok.

-type backend_priv() :: any().
-type lock_data() :: any().

-optional_callbacks([init/0,
                     pre_discovery/0,
                     post_discovery/1,
                     lock/1,
                     lock/2,
                     unlock/1,
                     unlock/2]).

-export_type([backend_priv/0,
              lock_data/0]).
