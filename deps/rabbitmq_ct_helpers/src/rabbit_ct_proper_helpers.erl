%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% Copyright (c) 2016-2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ct_proper_helpers).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([run_proper/3]).

run_proper(Fun, Args, NumTests) ->
    ?assertEqual(
       true,
       proper:counterexample(erlang:apply(Fun, Args),
			     [{numtests, NumTests},
			      {on_output, fun(".", _) -> ok; % don't print the '.'s on new lines
					     (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A) end}])).
