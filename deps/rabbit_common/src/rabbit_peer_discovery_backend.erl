%% This module is based on the autocluster_backend module
%% from rabbitmq-autocluster by Gavin Roy.
%%
%% Copyright (c) 2014-2015 AWeber Communications
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates
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
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

-callback lock(Node :: atom())   -> {ok, Data :: term()} | not_supported | {error, Reason :: string()}.

-callback unlock(Data :: term()) -> ok.

-optional_callbacks([init/0]).
