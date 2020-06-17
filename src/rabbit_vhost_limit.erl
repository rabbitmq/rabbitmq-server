%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_vhost_limit).

-behaviour(rabbit_runtime_parameter).

-include("rabbit.hrl").

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
                                           {user_who_performed_action, ActingUser}
                                           | Limits]),
    update_vhost(VHost, Limits).

notify_clear(VHost, <<"vhost-limits">>, <<"limits">>, ActingUser) ->
    rabbit_event:notify(vhost_limits_cleared, [{name, <<"limits">>},
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
    case rabbit_vhost_limit:connection_limit(VirtualHost) of
        %% no limit configured
        undefined                                            -> false;
        %% with limit = 0, no connections are allowed
        {ok, 0}                                              -> {true, 0};
        {ok, Limit} when is_integer(Limit) andalso Limit > 0 ->
            ConnectionCount = rabbit_connection_tracking:count_connections_in(VirtualHost),
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
                rabbit_misc:format("JSON decoding error. Reason: ~ts", [Reason])}
    end.

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
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              rabbit_vhost:update(VHostName,
                                  fun(VHost) ->
                                          rabbit_vhost:set_limits(VHost, Limits)
                                  end)
      end),
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
