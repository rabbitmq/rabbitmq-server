%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_vhost_limit).

-behaviour(rabbit_runtime_parameter).

-export([register/0]).
-export([parse_set/3, set/3, clear/2]).
-export([list/0, list/1]).
-export([update_limit/4, clear_limit/3, get_limit/2]).
-export([validate/5, notify/5, notify_clear/4]).
-export([connection_limit/1, queue_limit/1,
    is_over_queue_limit/1, would_exceed_queue_limit/2,
    is_over_connection_limit/1]).

-import(rabbit_misc, [pget/2, pget/3]).

-rabbit_boot_step({?MODULE,
                   [{description, "vhost limit parameters"},
                    {mfa, {rabbit_vhost_limit, register, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

%%----------------------------------------------------------------------------

register() ->
    rabbit_registry:register(runtime_parameter, <<"vhost-limits">>, ?MODULE).

validate(_VHost, <<"vhost-limits">>, Name, Term, _User) ->
    rabbit_parameter_validation:proplist(
      Name, vhost_limit_validation(), Term).

notify(VHost, <<"vhost-limits">>, <<"limits">>, Limits, ActingUser) ->
    rabbit_event:notify(vhost_limits_set, [{name, <<"limits">>},
                                           {vhost, VHost},
                                           {user_who_performed_action, ActingUser}
                                           | Limits]),
    update_vhost(VHost, Limits).

notify_clear(VHost, <<"vhost-limits">>, <<"limits">>, ActingUser) ->
    rabbit_event:notify(vhost_limits_cleared, [{name, <<"limits">>},
                                               {vhost, VHost},
                                               {user_who_performed_action, ActingUser}]),
    %% If the function is called as a part of vhost deletion, the vhost can
    %% be already deleted.
    case rabbit_vhost:exists(VHost) of
        true  -> update_vhost(VHost, undefined);
        false -> ok
    end.

connection_limit(VirtualHost) ->
    get_limit(VirtualHost, <<"max-connections">>).

queue_limit(VirtualHost) ->
    get_limit(VirtualHost, <<"max-queues">>).


query_limits(VHost) ->
    case rabbit_runtime_parameters:list(VHost, <<"vhost-limits">>) of
        []     -> [];
        Params -> [ {pget(vhost, Param), pget(value, Param)}
		    || Param <- Params,
		       pget(value, Param) =/= undefined,
		       pget(name, Param) == <<"limits">> ]
    end.


-spec list() -> [{vhost:name(), rabbit_types:infos()}].
list() ->
    query_limits('_').

-spec list(vhost:name()) -> rabbit_types:infos().
list(VHost) ->
    case query_limits(VHost) of
        []               -> [];
        [{VHost, Value}] -> Value
    end.

-spec is_over_connection_limit(vhost:name()) -> {true, non_neg_integer()} | false.

is_over_connection_limit(VirtualHost) ->
    case connection_limit(VirtualHost) of
        %% no limit configured
        undefined                                            -> false;
        %% with limit = 0, no connections are allowed
        {ok, 0}                                              -> {true, 0};
        {ok, Limit} when is_integer(Limit) andalso Limit > 0 ->
            ConnectionCount =
                rabbit_connection_tracking:count_tracked_items_in({vhost, VirtualHost}),
            case ConnectionCount >= Limit of
                false -> false;
                true  -> {true, Limit}
            end;
        %% any negative value means "no limit". Note that parameter validation
        %% will replace negative integers with 'undefined', so this is to be
        %% explicit and extra defensive
        {ok, Limit} when is_integer(Limit) andalso Limit < 0 -> false;
        %% ignore non-integer limits
        {ok, _Limit}                                         -> false
    end.

-spec would_exceed_queue_limit(non_neg_integer(), vhost:name()) ->
    {true, non_neg_integer(), non_neg_integer()} | false.

would_exceed_queue_limit(AdditionalCount, VirtualHost) ->
    case queue_limit(VirtualHost) of
        undefined ->
            %% no limit configured
            false;
        {ok, 0} ->
            %% with limit = 0, no queues can be declared (perhaps not very
            %% useful but consistent with the connection limit)
            {true, 0, 0};
        {ok, Limit} when is_integer(Limit) andalso Limit > 0 ->
            QueueCount = rabbit_amqqueue:count(VirtualHost),
            case (AdditionalCount + QueueCount) > Limit of
                false -> false;
                true  -> {true, Limit, QueueCount}
            end;
        {ok, Limit} when is_integer(Limit) andalso Limit < 0 ->
            %% any negative value means "no limit". Note that parameter validation
            %% will replace negative integers with 'undefined', so this is to be
            %% explicit and extra defensive
            false;
        {ok, _Limit} ->
            %% ignore non-integer limits
            false
    end.

-spec is_over_queue_limit(vhost:name()) -> {true, non_neg_integer()} | false.

is_over_queue_limit(VirtualHost) ->
    case would_exceed_queue_limit(1, VirtualHost) of
        {true, Limit, _QueueCount} -> {true, Limit};
        false -> false
    end.

%%----------------------------------------------------------------------------

parse_set(VHost, Defn, ActingUser) ->
    Definition = rabbit_data_coercion:to_binary(Defn),
    case rabbit_json:try_decode(Definition) of
        {ok, Term} ->
            set(VHost, maps:to_list(Term), ActingUser);
        {error, Reason} ->
            {error_string,
                rabbit_misc:format("Could not parse JSON document: ~tp", [Reason])}
    end.

-spec set(vhost:name(), [{binary(), binary()}], rabbit_types:user() | rabbit_types:username()) ->
    rabbit_runtime_parameters:ok_or_error_string().
set(VHost, Defn, ActingUser) ->
    rabbit_runtime_parameters:set_any(VHost, <<"vhost-limits">>,
                                      <<"limits">>, Defn, ActingUser).

clear(VHost, ActingUser) ->
    rabbit_runtime_parameters:clear_any(VHost, <<"vhost-limits">>,
                                        <<"limits">>, ActingUser).

update_limit(VHost, Name, Value, ActingUser) ->
    OldDef = case rabbit_runtime_parameters:list(VHost, <<"vhost-limits">>) of
        []      -> [];
        [Param] -> pget(value, Param, [])
    end,
    NewDef = [{Name, Value} | lists:keydelete(Name, 1, OldDef)],
    set(VHost, NewDef, ActingUser).

clear_limit(VHost, Name, ActingUser) ->
    OldDef = case rabbit_runtime_parameters:list(VHost, <<"vhost-limits">>) of
        []      -> [];
        [Param] -> pget(value, Param, [])
    end,
    NewDef = lists:keydelete(Name, 1, OldDef),
    set(VHost, NewDef, ActingUser).

vhost_limit_validation() ->
    [{<<"max-connections">>, fun rabbit_parameter_validation:integer/2, optional},
     {<<"max-queues">>,      fun rabbit_parameter_validation:integer/2, optional}].

update_vhost(VHostName, Limits) ->
    _ = rabbit_db_vhost:update(
          VHostName,
          fun(VHost) -> rabbit_vhost:set_limits(VHost, Limits) end),
    ok.

get_limit(VirtualHost, Limit) ->
    case rabbit_runtime_parameters:list(VirtualHost, <<"vhost-limits">>) of
        []      -> undefined;
        [Param] -> case pget(value, Param) of
                       undefined -> undefined;
                       Val       -> case pget(Limit, Val) of
                                        undefined     -> undefined;
                                        %% no limit
                                        N when N < 0  -> undefined;
                                        N when N >= 0 -> {ok, N}
                                    end
                   end
    end.
