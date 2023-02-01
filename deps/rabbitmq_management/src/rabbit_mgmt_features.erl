%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_features).

-export([is_edit_op_policy_disabled/0]).

is_edit_op_policy_disabled() ->
    case get_restriction([operator_policy_changes, disabled]) of
        true -> true;
        _ -> false
    end.

%% Private

get_restriction(Path) ->
    Restrictions = application:get_env(rabbitmq_management,  restrictions, []),
    rabbit_misc:deep_pget(Path, Restrictions, false).
