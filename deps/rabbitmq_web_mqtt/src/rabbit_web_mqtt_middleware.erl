%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_mqtt_middleware).
-behavior(cowboy_middleware).

-export([execute/2]).

execute(Req, Env) ->
    #{keepalive_sup := KeepaliveSup} = Env,
    case maps:get(handler_opts, Env, undefined) of
        undefined -> {ok, Req, Env};
        Opts when is_list(Opts) ->
            {ok, Req, Env#{handler_opts => [{keepalive_sup, KeepaliveSup}
                                            |Opts]}}
    end.
