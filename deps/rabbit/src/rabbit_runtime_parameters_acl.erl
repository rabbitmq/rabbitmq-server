%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_runtime_parameters_acl).

%% Access Control List for runtime parameters per virtual host. This provides
%% a generic framework for different parameters to support ACL. e.g. <<"policy">>.
%%
%% e.g. ACL entry, disallowing classic mirroring:
%% #runtime_parameters_acl{component       = {<<"/">>, <<"policy">>},
%%                         disallowed_list = [<<"ha-mode">>, ...]}


-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-export([table_name/0, create_table/0, ensure_table/0]).
-export([new/2, new/3, allow/4, disallow/4, is_allowed/3]).
-export([lookup/2, get_disallowed_list/2, format_error/3]).

-define(TAB, ?MODULE).

-spec table_name() -> atom().
table_name() -> ?TAB.

-spec create_table() -> rabbit_types:ok_or_error(any()).
create_table() ->
    try
        ClusterNodes = rabbit_mnesia:cluster_nodes(all),
        rabbit_table:create(?TAB,
            [{record_name, runtime_parameters_acl},
             {attributes, record_info(fields, runtime_parameters_acl)},
             {ram_copies, ClusterNodes}]),
        %% Ensure table type on each node corresponds to underlying node type
        [rpc:call(N, rabbit_table, change_table_copy_type, [?TAB]) ||
             N <- ClusterNodes],
        ok
    catch
        throw:Reason ->
            {error, Reason}
    end.

-spec new(rabbit_types:vhost(), binary()) -> rabbit_types:runtime_parameters_acl().
new(VHost, Component) ->
    new(VHost, Component, []).

-spec new(rabbit_types:vhost(), [binary()] | binary(), term()) ->
  rabbit_types:runtime_parameters_acl().
new(VHost, Component, Attributes) when is_list(Attributes) ->
    #runtime_parameters_acl{
        component       = {VHost, Component},
        disallowed_list = Attributes};
new(VHost, Component, Attribute) ->
    new(VHost, Component, [Attribute]).

-spec allow(rabbit_types:vhost(), binary(), any(), rabbit_types:username()) -> 'ok'.
allow(VHost, Component, Attribute, ActingUser) ->
    try is_supported(Attribute, Component) of
        true ->
            rabbit_misc:execute_mnesia_transaction(
                fun() ->
                    case mnesia:wread({?TAB, _Key = {VHost, Component}}) of
                        [#runtime_parameters_acl{disallowed_list = DL} = RPA] ->
                            rabbit_log:info("Allowing '~s' ~s in vhost '~s'",
                                            [Attribute, Component, VHost]),
                            RPA1 = RPA#runtime_parameters_acl{
                                       disallowed_list = lists:delete(Attribute, DL)},
                            ok = mnesia:write(?TAB, RPA1, write),
                            event_notify(parameter_allow, VHost, Component,
                                [{attribute, Attribute},
                                 {user_who_performed_action, ActingUser}]);
                        [] ->
                            ok
                    end
                end);
        false ->
            {error_string, rabbit_misc:format("'~s' is not a supported '~s' parameter",
                                              [Attribute, Component])};
        ignore ->
            ok
    catch
        _:_ ->
            {error_string, rabbit_misc:format("'~s' is not a supported '~s' parameter",
                                              [Attribute, Component])}
    end.

-spec disallow(rabbit_types:vhost(), binary(), any(), rabbit_types:username()) -> 'ok'.
disallow(VHost, Component, Attribute, ActingUser) ->
    try is_supported(Attribute, Component) of
        true ->
            rabbit_misc:execute_mnesia_transaction(
                fun() ->
                    rabbit_log:info("Disallowing '~s' ~s in vhost '~s'",
                                    [Attribute, Component, VHost]),
                    case mnesia:read(?TAB, Key = {VHost, Component}, read) of
                        [#runtime_parameters_acl{component = Key, disallowed_list = DL} = RPA] ->
                            RPA1 = RPA#runtime_parameters_acl{
                                       disallowed_list = lists:usort([Attribute | DL])},
                            ok = mnesia:write(?TAB, RPA1, write);
                        [] ->
                            RPA = new(VHost, Component, Attribute),
                            ok = mnesia:write(?TAB, RPA, write)
                    end,
                    event_notify(parameter_disallow, VHost, Component,
                        [{attribute, Attribute},
                         {user_who_performed_action, ActingUser}])
                end);
        false ->
            {error_string, rabbit_misc:format("'~s' is not a supported '~s' parameter",
                                              [Attribute, Component])};
        ignore ->
            ok
    catch
        _:_ ->
            {error_string, rabbit_misc:format("'~s' is not a supported '~s' parameter",
                                              [Attribute, Component])}
    end.

-spec is_allowed(rabbit_types:vhost(), binary(), any()) ->
  boolean() | {boolean(), any()}.
is_allowed(VHost, Component = <<"policy">>, Term) when is_list(Term) ->
    check_if_allowed(VHost, Component, Term);
is_allowed(VHost, Component, Attribute) when is_binary(Attribute) ->
    is_allowed(VHost, Component, [{<<"definition">>, [{Attribute, []}]}]);
%% Extensible, e.g. for operator-policies, shovel, federation, ... ACL rules.
is_allowed(_VHost, _Component, _Term) ->
    true.

-spec lookup(rabbit_types:vhost(), binary()) ->
  rabbit_types:ok_or_error2(rabbit_types:runtime_parameters_acl(), 'not_found').
lookup(VHost, Component) ->
    try
        rabbit_misc:dirty_read({?TAB, {VHost, Component}})
    catch
        _:_ -> {error, not_found}
    end.

-spec get_disallowed_list(rabbit_types:vhost(), binary()) -> list().
get_disallowed_list(VHost, Component) ->
    case lookup(VHost, Component) of
        {ok, #runtime_parameters_acl{disallowed_list = DL}} -> DL;
        {error, not_found}                                  -> []
    end.

-spec format_error(binary(), binary(), rabbit_types:vhost()) -> string().
format_error(<<"ha-mode">>, <<"policy">>, VHost) ->
    rabbit_misc:format("classic mirroring is not allowed in vhost '~s'", [VHost]);
format_error(Attribute, Component, VHost) ->
    rabbit_misc:format("'~s' ~s is not allowed in vhost '~s'",
        [Attribute, Component, VHost]).

%% This function is for test purposes, ensure runtime parameters ACL table still
%% exists if feature flag had been previously enabled following changes in mnesia
-spec ensure_table() -> 'ok'.
ensure_table() ->
    case rabbit_feature_flags:is_enabled(runtime_parameters_acl) of
        true ->
            case rabbit_table:exists(?TAB) of
                false ->
                    create_table();
                true ->
                    ok
            end;
        false ->
            ok
    end.

%% Internal

check_if_allowed(VHost, Component, Term) ->
    case rabbit_feature_flags:is_enabled(runtime_parameters_acl) of
        true ->
            case lookup(VHost, Component) of
                {ok, #runtime_parameters_acl{disallowed_list = DL}} ->
                    try
                        lists:map(
                            fun({Attribute, _Value}) ->
                                case lists:member(Attribute, DL) of
                                    true ->
                                        throw({not_allowed, Attribute});
                                    false ->
                                        ok
                                end
                            end, rabbit_misc:pget(<<"definition">>, Term, [])),
                        true
                    catch
                        throw:{not_allowed, Attribute} ->
                            {false, Attribute}
                    end;
                {error, not_found} ->
                    true
            end;
        false ->
            true
    end.

event_notify(Event, none, Component, Props) ->
    rabbit_event:notify(Event, [{component, Component} | Props]);
event_notify(Event, VHost, Component, Props) ->
    rabbit_event:notify(Event, [{vhost,     VHost},
                                {component, Component} | Props]).

is_supported(Attribute, Component) ->
    case rabbit_feature_flags:is_enabled(runtime_parameters_acl) of
        true ->
            Attribute1 = rabbit_data_coercion:to_list(string:lowercase(Attribute)),
            Attribute2 = list_to_existing_atom(Attribute1),
            proplists:is_defined(Attribute2, get_supported_attributes(Component));
        false ->
            ignore
    end.

get_supported_attributes(<<"policy">>) ->
    rabbit_registry:lookup_all(policy_validator).
