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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_parameters).
-behaviour(rabbit_runtime_parameter).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([validate/3, validate_clear/2, notify/3, notify_clear/2]).
-export([register/0]).

-rabbit_boot_step({?MODULE,
                   [{description, "federation parameters"},
                    {mfa, {rabbit_federation_parameters, register, []}},
                    {mfa, {rabbit_registry, register,
                           [runtime_parameter, <<"federation">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    [rabbit_registry:register(runtime_parameter, Name, ?MODULE) ||
        Name <- [<<"federation">>,
                 <<"federation_connection">>,
                 <<"federation_upstream_set">>]].

%% TODO: don't allow upstream set to be created referencing
%% connections that do not exist?
validate(<<"federation_upstream_set">>, _Key, Term) ->
    [assert_contents([{<<"connection">>, binary, mandatory},
                      {<<"exchange">>,   binary, optional} |
                      connection_upstream_set_validation()], Upstream)
     || Upstream <- Term];

validate(<<"federation_connection">>, _Key, Term) ->
    assert_contents([{<<"host">>, binary, mandatory} |
                     connection_upstream_set_validation()], Term);

validate(<<"federation">>, <<"local_nodename">>, Term) ->
    assert_type(<<"local_nodename">>, binary, Term);

validate(<<"federation">>, <<"local_username">>, Term) ->
    assert_type(<<"local_username">>, binary, Term);

validate(_AppName, Key, _Term) ->
    {error, "key not recognised: ~p", [Key]}.

validate_clear(<<"federation_upstream_set">>, _Key) ->
    ok;

%% TODO: don't allow connection to be removed if upstream sets reference it?
validate_clear(<<"federation_connection">>, _Key) ->
    ok;

validate_clear(<<"federation">>, <<"local_nodename">>) ->
    ok;

validate_clear(<<"federation">>, <<"local_username">>) ->
    ok;

validate_clear(_AppName, Key) ->
    {error, "key not recognised: ~p", [Key]}.

notify(<<"federation_upstream_set">>, Key, _Term) ->
    rabbit_federation_link_sup_sup:adjust({upstream_set, Key});

notify(<<"federation_connection">>, Key, _Term) ->
    rabbit_federation_link_sup_sup:adjust({connection, Key});

notify(<<"federation">>, <<"local_nodename">>, _Term) ->
    rabbit_federation_link_sup_sup:adjust(everything);

notify(<<"federation">>, <<"local_username">>, _Term) ->
    rabbit_federation_link_sup_sup:adjust(everything).

notify_clear(<<"federation_upstream_set">>, Key) ->
    rabbit_federation_link_sup_sup:adjust({clear_upstream_set, Key});

notify_clear(<<"federation_connection">>, Key) ->
    rabbit_federation_link_sup_sup:adjust({clear_connection, Key});

notify_clear(<<"federation">>, <<"local_nodename">>) ->
    rabbit_federation_link_sup_sup:adjust(everything);

notify_clear(<<"federation">>, <<"local_username">>) ->
    rabbit_federation_link_sup_sup:adjust(everything).

%%----------------------------------------------------------------------------

assert_type(Name, {Type, Opts}, Term) ->
    assert_type(Name, Type, Term),
    case lists:member(Term, Opts) of
        true  -> ok;
        false -> {error, "~s must be one of ~p", [Name, Opts]}
    end;

assert_type(_Name, number, Term) when is_number(Term) ->
    ok;

assert_type(Name, number, Term) ->
    {error, "~s should be number, actually was ~p", [Name, Term]};

assert_type(_Name, binary, Term) when is_binary(Term) ->
    ok;

assert_type(Name, binary, Term) ->
    {error, "~s should be binary, actually was ~p", [Name, Term]}.

connection_upstream_set_validation() ->
    [{<<"port">>,            number, optional},
     {<<"protocol">>,        {binary, [<<"amqp">>, <<"amqps">>]}, optional},
     {<<"virtual_host">>,    binary, optional},
     {<<"username">>,        binary, optional},
     {<<"password">>,        binary, optional},
     {<<"exchange">>,        binary, optional},
     {<<"prefetch_count">>,  number, optional},
     {<<"reconnect_delay">>, number, optional},
     {<<"max_hops">>,        number, optional},
     {<<"expires">>,         number, optional},
     {<<"message_ttl">>,     number, optional},
     {<<"ha_policy">>,       binary, optional}].

assert_contents(Constraints, Term) ->
    {Results, Remainder}
        = lists:foldl(
            fun ({Name, Constraint, Needed}, {Results0, Term0}) ->
                    case {lists:keytake(Name, 1, Term0), Needed} of
                        {{value, {Name, Value}, Term1}, _} ->
                            {[assert_type(Name, Constraint, Value) | Results0],
                             Term1};
                        {false, mandatory} ->
                            {[{error, "Key \"~s\" not found", [Name]} |
                              Results0], Term0};
                        {false, optional} ->
                            {Results0, Term0}
                    end
            end, {[], Term}, Constraints),
    case Remainder of
        [] -> Results;
        _  -> [{error, "Unrecognised terms ~p", [Remainder]} | Results]
    end.
