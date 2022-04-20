%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_ct_proper_helpers).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([run_proper/3]).

run_proper(Fun, Args, NumTests) ->
    ?assert(
       proper:counterexample(erlang:apply(Fun, Args),
			     [{numtests, NumTests},
			      {on_output, fun(".", _) -> ok; % don't print the '.'s on new lines
					     (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A) end}])).
