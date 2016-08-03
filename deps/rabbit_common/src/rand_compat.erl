%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rand_compat).

%% We don't want warnings about the use of erlang:now/0 in
%% this module.
-compile(nowarn_deprecated_function).

%% Declare versioned functions to allow dynamic code loading,
%% depending on the Erlang version running. See 'code_version.erl' for details
-erlang_version_support([
    {18, [
        {seed, 1, seed_pre_18, seed_post_18},
        {seed, 2, seed_pre_18, seed_post_18},
        {uniform, 0, uniform_pre_18, uniform_post_18},
        {uniform, 1, uniform_pre_18, uniform_post_18},
        {uniform_s, 1, uniform_s_pre_18, uniform_s_post_18},
        {uniform_s, 2, uniform_s_pre_18, uniform_s_post_18}
      ]}
  ]).

-export([
    seed/1, seed_pre_18/1, seed_post_18/1,
    seed/2, seed_pre_18/2, seed_post_18/2,
    uniform/0, uniform_pre_18/0, uniform_post_18/0,
    uniform/1, uniform_pre_18/1, uniform_post_18/1,
    uniform_s/1, uniform_s_pre_18/1, uniform_s_post_18/1,
    uniform_s/2, uniform_s_pre_18/2, uniform_s_post_18/2
  ]).

-define(IS_ALG(A), (A =:= exs64 orelse A =:= exsplus orelse A =:= exs1024)).

%% export_seed_s/1 can't be implemented with `random`.
%% export_seed_s/2. can't be implemented with `random`.

%% normal_s/1 can't be implemented with `random`.
%% normal_s/2. can't be implemented with `random`.

%% seed/1.

seed(AlgOrExpState) ->
    code_version:update(?MODULE),
    ?MODULE:seed(AlgOrExpState).

seed_pre_18(Alg) when ?IS_ALG(Alg) -> random:seed();
seed_pre_18(ExpState)              -> random:seed(ExpState).
seed_post_18(AlgOrExpState)        -> rand:seed(AlgOrExpState).

%% seed/2.

seed(Alg, ExpState) ->
    code_version:update(?MODULE),
    ?MODULE:seed(Alg, ExpState).

seed_pre_18(_Alg, ExpState) -> random:seed(ExpState).
seed_post_18(Alg, ExpState) -> rand:seed(Alg, ExpState).

%% seed_s/1 can't be implemented with `random`.
%% seed_s/2. can't be implemented with `random`.

%% uniform/0.

uniform() ->
    code_version:update(?MODULE),
    ?MODULE:uniform().

ensure_random_seed() ->
    case get(random_seed) of
        undefined ->
            random:seed(erlang:phash2([node()]),
                        time_compat:monotonic_time(),
                        time_compat:unique_integer());
        _ -> ok
    end.

uniform_pre_18()  ->
    ensure_random_seed(),
    random:uniform().

uniform_post_18() -> rand:uniform().

%% uniform/1.

uniform(N) ->
    code_version:update(?MODULE),
    ?MODULE:uniform(N).

uniform_pre_18(N)  ->
    ensure_random_seed(),
    random:uniform(N).

uniform_post_18(N) -> rand:uniform(N).

%% uniform_s/1.

uniform_s(State) ->
    code_version:update(?MODULE),
    ?MODULE:uniform_s(State).

uniform_s_pre_18(State)  -> random:uniform_s(State).
uniform_s_post_18(State) -> rand:uniform_s(State).

%% uniform_s/2.

uniform_s(N, State) ->
    code_version:update(?MODULE),
    ?MODULE:uniform_s(N, State).

uniform_s_pre_18(N, State)  -> random:uniform_s(N, State).
uniform_s_post_18(N, State) -> rand:uniform_s(N, State).
