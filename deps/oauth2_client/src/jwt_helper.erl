%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(jwt_helper).

-export([decode/1, get_expiration/1]).

-include_lib("jose/include/jose_jwt.hrl").

decode(Token) ->
    try
        #jose_jwt{fields = Fields} = jose_jwt:peek_payload(Token),
        Fields
    catch Type:Err:Stacktrace ->
        {error, {invalid_token, Type, Err, Stacktrace}}
    end.

get_expiration(#{<<"exp">> := Exp}) when is_integer(Exp) -> {ok, Exp};
get_expiration(#{}) -> {error, missing_exp_field}.
