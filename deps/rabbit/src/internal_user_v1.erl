%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(internal_user_v1).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([
  new/0,
  new/1,
  record_version_to_use/0,
  fields/0,
  fields/1,
  upgrade/1,
  upgrade_to/2,
  pattern_match_all/0,
  get_username/1,
  get_password_hash/1,
  get_tags/1,
  get_hashing_algorithm/1,
  get_limits/1,
  create_user/3,
  set_password_hash/3,
  set_tags/2,
  update_limits/3,
  clear_limits/1
]).

-define(record_version, ?MODULE).

-record(internal_user, {
    username :: internal_user:username() | '_',
    password_hash :: internal_user:password_hash() | '_',
    tags :: [atom()] | '_',
    %% password hashing implementation module,
    %% typically rabbit_password_hashing_* but can
    %% come from a plugin
    hashing_algorithm :: atom() | '_'}).

-type internal_user() :: internal_user_v1().

-type(internal_user_v1() ::
        #internal_user{username          :: internal_user:username(),
                       password_hash     :: internal_user:password_hash(),
                       tags              :: [atom()],
                       hashing_algorithm :: atom()}).

-type internal_user_pattern() :: internal_user_v1_pattern().

-type internal_user_v1_pattern() :: #internal_user{
                                                  username :: internal_user:username() | '_',
                                                  password_hash :: '_',
                                                  tags :: '_',
                                                  hashing_algorithm :: '_'
                                                 }.

-export_type([internal_user/0,
              internal_user_v1/0,
              internal_user_pattern/0,
              internal_user_v1_pattern/0]).

-spec record_version_to_use() -> internal_user_v1.
record_version_to_use() ->
    ?record_version.

-spec new() -> internal_user().
new() ->
    #internal_user{
        username = <<"">>,
        password_hash = <<"">>,
        tags = []
    }.

-spec new(tuple()) -> internal_user().
new({hashing_algorithm, HashingAlgorithm}) ->
    #internal_user{
        username = <<"">>,
        password_hash = <<"">>,
        hashing_algorithm = HashingAlgorithm,
        tags = []
    };
new({tags, Tags}) ->
    #internal_user{
        username = <<"">>,
        password_hash = <<"">>,
        tags = Tags
    }.

-spec fields() -> list().
fields() -> fields(?record_version).

-spec fields(atom()) -> list().
fields(?record_version) -> record_info(fields, internal_user).

-spec upgrade(internal_user()) -> internal_user().
upgrade(#internal_user{} = User) -> User.

-spec upgrade_to(internal_user_v1, internal_user()) -> internal_user().
upgrade_to(?record_version, #internal_user{} = User) ->
    User.

-spec pattern_match_all() -> internal_user_pattern().
pattern_match_all() -> #internal_user{_ = '_'}.

-spec get_username(internal_user()) -> internal_user:username().
get_username(#internal_user{username = Value}) -> Value.

-spec get_password_hash(internal_user()) -> internal_user:password_hash().
get_password_hash(#internal_user{password_hash = Value}) -> Value.

-spec get_tags(internal_user()) -> [atom()].
get_tags(#internal_user{tags = Value}) -> Value.

-spec get_hashing_algorithm(internal_user()) -> atom().
get_hashing_algorithm(#internal_user{hashing_algorithm = Value}) -> Value.

-spec get_limits(internal_user()) -> map().
get_limits(_User) -> #{}.

-spec create_user(internal_user:username(), internal_user:password_hash(),
    atom()) -> internal_user().
create_user(Username, PasswordHash, HashingMod) ->
    #internal_user{username = Username,
                   password_hash = PasswordHash,
                   tags = [],
                   hashing_algorithm = HashingMod
                  }.

-spec set_password_hash(internal_user:internal_user(),
    internal_user:password_hash(), atom()) -> internal_user().
set_password_hash(#internal_user{} = User, PasswordHash, HashingAlgorithm) ->
    User#internal_user{password_hash = PasswordHash,
                       hashing_algorithm = HashingAlgorithm}.

-spec set_tags(internal_user(), [atom()]) -> internal_user().
set_tags(#internal_user{} = User, Tags) ->
    User#internal_user{tags = Tags}.

-spec update_limits
(add, internal_user(), map()) -> internal_user();
(remove, internal_user(), term()) -> internal_user().
update_limits(_, User, _) ->
    User.

-spec clear_limits(internal_user()) -> internal_user().
clear_limits(User) ->
    User.
