%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_vhost_defaults).

-export([apply/2]).
-export([list_limits/1, list_operator_policies/1, list_users/1]).

-type definitions() :: [{binary(), term()}].

-record(seeding_policy, {
    name                     :: binary(),
    queue_pattern = <<".*">> :: binary(),
    definition    = []       :: definitions()
}).

-type seeded_user_properties() :: #{
    name      := binary(),
    configure := binary(),
    read      := binary(),
    write     := binary(),
    password  := binary(),
    tags      := [atom()],
    _         => _
}.

%% Apply all matching defaults to a VHost.
-spec apply(vhost:name(), rabbit_types:username()) -> ok.
apply(VHost, ActingUser) ->
    case list_limits(VHost) of
        [] ->
            ok;
        L  ->
            ok = rabbit_vhost_limit:set(VHost, L, ActingUser),
            rabbit_log:info("Applied default limits to vhost '~tp': ~tp", [VHost, L])
    end,
    lists:foreach(
        fun(P) ->
            ok = rabbit_policy:set_op(VHost, P#seeding_policy.name, P#seeding_policy.queue_pattern, P#seeding_policy.definition,
                                      undefined, undefined, ActingUser),
            rabbit_log:info("Applied default operator policy to vhost '~tp': ~tp", [VHost, P])
        end,
        list_operator_policies(VHost)
    ),
    lists:foreach(
        fun(U) ->
            ok = add_user(VHost, U, ActingUser),
            rabbit_log:info("Added default user to vhost '~tp': ~tp", [VHost, maps:remove(password, U)])
        end,
        list_users(VHost)
    ),
    ok.

%%
%% Helpers
%%

%% Limits that were configured with a matching vhost pattern.
-spec list_limits(vhost:name()) -> proplists:proplist().
list_limits(VHost) ->
    AllLimits = application:get_env(rabbit, default_limits, []),
    VHostLimits = proplists:get_value(vhosts, AllLimits, []),
    Match = lists:search(
        fun({_, Ss}) ->
            RE = proplists:get_value(<<"pattern">>, Ss, ".*"),
            re:run(VHost, RE, [{capture, none}]) =:= match
        end,
        VHostLimits
    ),
    case Match of
        {value, {_, Ss}} ->
            Ss1 = proplists:delete(<<"pattern">>, Ss),
            underscore_to_dash(Ss1);
        _ ->
            []
    end.

%% Operator policies that were configured with a matching vhost pattern.
-spec list_operator_policies(vhost:name()) -> [#seeding_policy{}].
list_operator_policies(VHost) ->
    AllPolicies = application:get_env(rabbit, default_policies, []),
    OpPolicies = proplists:get_value(operator, AllPolicies, []),
    lists:filtermap(
        fun({PolicyName, Ss}) ->
            RE = proplists:get_value(<<"vhost_pattern">>, Ss, ".*"),
            case re:run(VHost, RE, [{capture, none}]) of
                match ->
                    QPattern = proplists:get_value(<<"queue_pattern">>, Ss, <<".*">>),
                    Ss1 = proplists:delete(<<"queue_pattern">>, Ss),
                    Ss2 = proplists:delete(<<"vhost_pattern">>, Ss1),
                    {true, #seeding_policy{
                        name = PolicyName,
                        queue_pattern = QPattern,
                        definition = underscore_to_dash(Ss2)
                    }};
                _ ->
                    false
            end
        end,
        OpPolicies
    ).

%% Users (permissions) that were configured with a matching vhost pattern.
-spec list_users(vhost:name()) -> [seeded_user_properties()].
list_users(VHost) ->
    Users = application:get_env(rabbit, default_users, []),
    lists:filtermap(
        fun({Username, Ss}) ->
            RE = proplists:get_value(<<"vhost_pattern">>, Ss, ".*"),
            case re:run(VHost, RE, [{capture, none}]) of
                match ->
                    C = rabbit_data_coercion:to_binary(
                              proplists:get_value(<<"configure">>, Ss, <<".*">>)
                            ),
                    R = rabbit_data_coercion:to_binary(
                              proplists:get_value(<<"read">>, Ss, <<".*">>)
                            ),
                    W = rabbit_data_coercion:to_binary(
                              proplists:get_value(<<"write">>, Ss, <<".*">>)
                            ),
                    U0 = #{
                            name      => Username,
                            tags      => proplists:get_value(<<"tags">>, Ss, []),
                            configure => C,
                            read      => R,
                            write     => W
                          },
                    %% rabbit_auth_backend_internal:put_user relies on maps:is_key, can't pass
                    %% undefined through.
                    U1 = case proplists:get_value(<<"password">>, Ss, undefined) of
                            undefined ->
                                U0;
                            V ->
                                U0#{password => rabbit_data_coercion:to_binary(V)}
                         end,
                    {true, U1};
                _ ->
                    false
            end
        end,
        Users
    ).

%%
%% Private
%%

%% Translate underscores to dashes in prop keys.
-spec underscore_to_dash(definitions()) -> definitions().
underscore_to_dash(Props) ->
    lists:map(
        fun({N, V}) ->
            {binary:replace(N, <<"_">>, <<"-">>, [global]), V}
        end,
        Props
    ).

%% Add user iff it doesn't exist & set permissions per vhost.
-spec add_user(rabbit_types:vhost(), seeded_user_properties(), rabbit_types:username()) -> ok.
add_user(VHost, #{name := Name, configure := C, write := W, read := R} = User, ActingUser) ->
    %% put_user has its own existence check, but it still updates password if the user exists.
    %% We want only the newly created users to have password set from the config.
    rabbit_auth_backend_internal:exists(Name) orelse
        rabbit_auth_backend_internal:put_user(User, ActingUser),
    rabbit_auth_backend_internal:set_permissions(Name, VHost, C, W, R, ActingUser).
