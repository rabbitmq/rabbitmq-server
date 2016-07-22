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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_vhost_limit).

-behaviour(rabbit_runtime_parameter).

-include("rabbit.hrl").

-export([register/0]).
-export([parse_set/2, clear/1]).
-export([validate/5, notify/4, notify_clear/3]).
-export([connection_limit/1]).

-import(rabbit_misc, [pget/2]).

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

notify(VHost, <<"vhost-limits">>, <<"limits">>, Limits) ->
    rabbit_event:notify(vhost_limits_set, [{name, <<"limits">>} | Limits]),
    update_vhost(VHost, Limits).

notify_clear(VHost, <<"vhost-limits">>, <<"limits">>) ->
    rabbit_event:notify(vhost_limits_cleared, [{name, <<"limits">>}]),
    update_vhost(VHost, undefined).

connection_limit(VirtualHost) ->
    get_limit(VirtualHost, <<"max-connections">>).

%%----------------------------------------------------------------------------

parse_set(VHost, Defn) ->
    case rabbit_misc:json_decode(Defn) of
        {ok, JSON} ->
            set(VHost, rabbit_misc:json_to_term(JSON));
        error ->
            {error_string, "JSON decoding error"}
    end.

set(VHost, Defn) ->
    rabbit_runtime_parameters:set_any(VHost, <<"vhost-limits">>,
                                      <<"limits">>, Defn, none).

clear(VHost) ->
    rabbit_runtime_parameters:clear_any(VHost, <<"vhost-limits">>,
                                        <<"limits">>).

vhost_limit_validation() ->
    [{<<"max-connections">>, fun rabbit_parameter_validation:number/2, mandatory}].

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
                                        N when N =< 0 -> undefined;
                                        N when N > 0  -> {ok, N}
                                    end
                   end
    end.
