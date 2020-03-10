%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2017-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% Dummy module to test rabbit_direct:extract_extra_auth_props

-module(rabbit_dummy_protocol_connection_info).

%% API
-export([additional_authn_params/4]).

additional_authn_params(_Creds, _VHost, Pid, _Infos) ->
    case Pid of
        -1 -> throw(error);
        _  -> [{client_id, <<"DummyClientId">>}]
    end.
