%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stomp_connection_info).

%% Note: this is necessary to prevent code:get_object_code from
%% backing up due to a missing module. See VESC-888.

%% API
-export([additional_authn_params/4]).

additional_authn_params(_Creds, _VHost, _Pid, _Infos) ->
    [].
