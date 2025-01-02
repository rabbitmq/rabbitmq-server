%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_credential_validator).

%% Validates a password. Used by `rabbit_auth_backend_internal`.
%%
%% Possible return values:
%%
%% * ok: provided password passed validation.
%% * {error, Error, Args}: provided password password failed validation.

-callback validate(rabbit_types:username(), rabbit_types:password()) -> 'ok' | {'error', string()}.
