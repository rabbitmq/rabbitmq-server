%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
