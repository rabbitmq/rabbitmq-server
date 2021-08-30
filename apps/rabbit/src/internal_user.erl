%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(internal_user).

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

-define(record_version, internal_user_v2).

-type(username() :: binary()).

-type(password_hash() :: binary()).

-type internal_user() :: internal_user_v1:internal_user_v1() | internal_user_v2().

-record(internal_user, {
    username :: username() | '_',
    password_hash :: password_hash() | '_',
    tags :: [atom()] | '_',
    %% password hashing implementation module,
    %% typically rabbit_password_hashing_* but can
    %% come from a plugin
    hashing_algorithm :: atom() | '_',
    limits = #{} :: map() | '_'}).

-type(internal_user_v2() ::
        #internal_user{username          :: username() | '_',
                       password_hash     :: password_hash() | '_',
                       tags              :: [atom()] | '_',
                       hashing_algorithm :: atom() | '_',
                       limits            :: map()}).

-type internal_user_pattern() :: internal_user_v1:internal_user_v1_pattern() |
                                 internal_user_v2_pattern().

-type internal_user_v2_pattern() :: #internal_user{
                                                  username :: username() | '_',
                                                  password_hash :: '_',
                                                  tags :: '_',
                                                  hashing_algorithm :: '_',
                                                  limits :: '_'
                                                 }.

-export_type([username/0,
              password_hash/0,
              internal_user/0,
              internal_user_v2/0,
              internal_user_pattern/0,
              internal_user_v2_pattern/0]).

-spec new() -> internal_user().
new() ->
    case record_version_to_use() of
        ?record_version ->
            #internal_user{
                username = <<"">>,
                password_hash = <<"">>,
                tags = []
            };
        _ ->
            internal_user_v1:new()
    end.

-spec new(tuple()) -> internal_user().
new({hashing_algorithm, HashingAlgorithm}) ->
    case record_version_to_use() of
        ?record_version ->
            #internal_user{
                username = <<"">>,
                password_hash = <<"">>,
                tags = [],
                hashing_algorithm = HashingAlgorithm
            };
        _ ->
            internal_user_v1:new({hashing_algorithm, HashingAlgorithm})
    end;
new({tags, Tags}) ->
    case record_version_to_use() of
	?record_version ->
	    #internal_user{
            username = <<"">>,
            password_hash = <<"">>,
            tags = Tags
        };
	_ ->
	    internal_user_v1:new({tags, Tags})
    end.

-spec record_version_to_use() -> internal_user_v1 | internal_user_v2.
record_version_to_use() ->
    case rabbit_feature_flags:is_enabled(user_limits) of
        true  -> ?record_version;
        false -> internal_user_v1:record_version_to_use()
    end.

-spec fields() -> list().
fields() ->
    case record_version_to_use() of
        ?record_version -> fields(?record_version);
        _               -> internal_user_v1:fields()
    end.

-spec fields(atom()) -> list().
fields(?record_version) -> record_info(fields, internal_user);
fields(Version)         -> internal_user_v1:fields(Version).

-spec upgrade(internal_user()) -> internal_user().
upgrade(#internal_user{} = User) -> User;
upgrade(OldUser)         -> upgrade_to(record_version_to_use(), OldUser).

-spec upgrade_to
(internal_user_v2, internal_user()) -> internal_user_v2();
(internal_user_v1, internal_user_v1:internal_user_v1()) -> internal_user_v1:internal_user_v1().

upgrade_to(?record_version, #internal_user{} = User) ->
    User;
upgrade_to(?record_version, OldUser) ->
    Fields = erlang:tuple_to_list(OldUser) ++ [#{}],
    #internal_user{} = erlang:list_to_tuple(Fields);
upgrade_to(Version, OldUser) ->
    internal_user_v1:upgrade_to(Version, OldUser).

-spec pattern_match_all() -> internal_user_pattern().
pattern_match_all() ->
    case record_version_to_use() of
        ?record_version -> #internal_user{_ = '_'};
        _               -> internal_user_v1:pattern_match_all()
    end.

-spec get_username(internal_user()) -> username().
get_username(#internal_user{username = Value}) -> Value;
get_username(User) -> internal_user_v1:get_username(User).

-spec get_password_hash(internal_user()) -> password_hash().
get_password_hash(#internal_user{password_hash = Value}) -> Value;
get_password_hash(User) -> internal_user_v1:get_password_hash(User).

-spec get_tags(internal_user()) -> [atom()].
get_tags(#internal_user{tags = Value}) -> Value;
get_tags(User) -> internal_user_v1:get_tags(User).

-spec get_hashing_algorithm(internal_user()) -> atom().
get_hashing_algorithm(#internal_user{hashing_algorithm = Value}) -> Value;
get_hashing_algorithm(User) -> internal_user_v1:get_hashing_algorithm(User).

-spec get_limits(internal_user()) -> map().
get_limits(#internal_user{limits = Value}) -> Value;
get_limits(User) -> internal_user_v1:get_limits(User).

-spec create_user(username(), password_hash(), atom()) -> internal_user().
create_user(Username, PasswordHash, HashingMod) ->
    case record_version_to_use() of
        ?record_version ->
            #internal_user{username = Username,
                           password_hash = PasswordHash,
                           tags = [],
                           hashing_algorithm = HashingMod,
                           limits = #{}
                          };
        _ ->
            internal_user_v1:create_user(Username, PasswordHash, HashingMod)
    end.

-spec set_password_hash(internal_user(), password_hash(), atom()) -> internal_user().
set_password_hash(#internal_user{} = User, PasswordHash, HashingAlgorithm) ->
    User#internal_user{password_hash = PasswordHash,
                       hashing_algorithm = HashingAlgorithm};
set_password_hash(User, PasswordHash, HashingAlgorithm) ->
    internal_user_v1:set_password_hash(User, PasswordHash, HashingAlgorithm).

-spec set_tags(internal_user(), [atom()]) -> internal_user().
set_tags(#internal_user{} = User, Tags) ->
    User#internal_user{tags = Tags};
set_tags(User, Tags) ->
    internal_user_v1:set_tags(User, Tags).

-spec update_limits
(add, internal_user(), map()) -> internal_user();
(remove, internal_user(), term()) -> internal_user().
update_limits(add, #internal_user{limits = Limits} = User, Term) ->
    User#internal_user{limits = maps:merge(Limits, Term)};
update_limits(remove, #internal_user{limits = Limits} = User, LimitType) ->
    User#internal_user{limits = maps:remove(LimitType, Limits)};
update_limits(Action, User, Term) ->
    internal_user_v1:update_limits(Action, User, Term).

-spec clear_limits(internal_user()) -> internal_user().
clear_limits(#internal_user{} = User) ->
    User#internal_user{limits = #{}};
clear_limits(User) ->
    internal_user_v1:clear_limits(User).
