%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(term_to_binary_compat).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([term_to_binary_1/1]).

term_to_binary_1(Term) ->
    term_to_binary(Term, [{minor_version, 1}]).
