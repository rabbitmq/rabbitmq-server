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
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_definitions).
-include_lib("rabbit_common/include/rabbit.hrl").

-export([maybe_load_definitions/0, maybe_load_definitions_from/2]).
-export([apply_defs/4, apply_defs/5]).
-export([decode/1, decode/2, args/1]).

maybe_load_definitions() ->
    %% this feature was a part of rabbitmq-management for a long time,
    %% so we check rabbit_management.load_definitions for backward compatibility.
    maybe_load_management_definitions(),
    %% this backs "core" load_definitions
    maybe_load_core_definitions().

maybe_load_core_definitions() ->
    maybe_load_definitions(rabbit, load_definitions).

maybe_load_management_definitions() ->
    maybe_load_definitions(rabbitmq_management, load_definitions).

maybe_load_definitions(App, Key) ->
    case application:get_env(App, Key) of
        undefined  -> ok;
        {ok, none} -> ok;
        {ok, FileOrDir} ->
            IsDir = filelib:is_dir(FileOrDir),
            maybe_load_definitions_from(IsDir, FileOrDir)
    end.

maybe_load_definitions_from(true, Dir) ->
    rabbit_log:info("Applying definitions from directory ~s", [Dir]),
    load_definitions_from_files(file:list_dir(Dir), Dir);
maybe_load_definitions_from(false, File) ->
    load_definitions_from_file(File).

load_definitions_from_files({ok, Filenames0}, Dir) ->
    Filenames1 = lists:sort(Filenames0),
    Filenames2 = [filename:join(Dir, F) || F <- Filenames1],
    load_definitions_from_filenames(Filenames2);
load_definitions_from_files({error, E}, Dir) ->
    rabbit_log:error("Could not read definitions from directory ~s, Error: ~p", [Dir, E]),
    {error, {could_not_read_defs, E}}.

load_definitions_from_filenames([]) ->
    ok;
load_definitions_from_filenames([File|Rest]) ->
    case load_definitions_from_file(File) of
        ok         -> load_definitions_from_filenames(Rest);
        {error, E} -> {error, {failed_to_import_definitions, File, E}}
    end.

load_definitions_from_file(File) ->
    case file:read_file(File) of
        {ok, Body} ->
            rabbit_log:info("Applying definitions from ~s", [File]),
            load_definitions(Body);
        {error, E} ->
            rabbit_log:error("Could not read definitions from ~s, Error: ~p", [File, E]),
            {error, {could_not_read_defs, {File, E}}}
    end.

load_definitions(Body) ->
    apply_defs(
        Body, ?INTERNAL_USER,
        fun () -> ok end,
        fun (E) -> {error, E} end).

decode(Keys, Body) ->
    case decode(Body) of
        {ok, J0} ->
                    J = maps:fold(fun(K, V, Acc) ->
                        Acc#{binary_to_atom(K, utf8) => V}
                    end, J0, J0),
                    Results = [get_or_missing(K, J) || K <- Keys],
                    case [E || E = {key_missing, _} <- Results] of
                        []      -> {ok, Results, J};
                        Errors  -> {error, Errors}
                    end;
        Else     -> Else
    end.

decode(<<"">>) ->
    {ok, #{}};
decode(Body) ->
    try
        {ok, rabbit_json:decode(Body)}
    catch error:_ -> {error, not_json}
    end.

apply_defs(Body, ActingUser, SuccessFun, ErrorFun) ->
    rabbit_log:info("Asked to import definitions. Acting user: ~s", [rabbit_data_coercion:to_binary(ActingUser)]),
    case decode([], Body) of
        {error, E} ->
            ErrorFun(E);
        {ok, _, All} ->
            Version = maps:get(rabbit_version, All, undefined),
            try
                rabbit_log:info("Importing users..."),
                for_all(users,              ActingUser, All,
                        fun(User, _Username) ->
                                rabbit_auth_backend_internal:put_user(
                                  User,
                                  Version,
                                  ActingUser)
                        end),
                rabbit_log:info("Importing vhosts..."),
                for_all(vhosts,             ActingUser, All, fun add_vhost/2),
                validate_limits(All),
                rabbit_log:info("Importing user permissions..."),
                for_all(permissions,        ActingUser, All, fun add_permission/2),
                rabbit_log:info("Importing topic permissions..."),
                for_all(topic_permissions,  ActingUser, All, fun add_topic_permission/2),
                rabbit_log:info("Importing parameters..."),
                for_all(parameters,         ActingUser, All, fun add_parameter/2),
                rabbit_log:info("Importing global parameters..."),
                for_all(global_parameters,  ActingUser, All, fun add_global_parameter/2),
                rabbit_log:info("Importing policies..."),
                for_all(policies,           ActingUser, All, fun add_policy/2),
                rabbit_log:info("Importing queues..."),
                for_all(queues,             ActingUser, All, fun add_queue/2),
                rabbit_log:info("Importing exchanges..."),
                for_all(exchanges,          ActingUser, All, fun add_exchange/2),
                rabbit_log:info("Importing bindings..."),
                for_all(bindings,           ActingUser, All, fun add_binding/2),
                SuccessFun()
            catch {error, E} -> ErrorFun(format(E));
                  exit:E     -> ErrorFun(format(E))
            end
    end.

apply_defs(Body, ActingUser, SuccessFun, ErrorFun, VHost) ->
    rabbit_log:info("Asked to import definitions for a virtual host. Virtual host: ~p, acting user: ~p",
                    [VHost, ActingUser]),
    case decode([], Body) of
        {error, E} ->
            ErrorFun(E);
        {ok, _, All} ->
            try
                validate_limits(All, VHost),
                rabbit_log:info("Importing parameters..."),
                for_all(parameters,  ActingUser, All, VHost, fun add_parameter/3),
                rabbit_log:info("Importing policies..."),
                for_all(policies,    ActingUser, All, VHost, fun add_policy/3),
                rabbit_log:info("Importing queues..."),
                for_all(queues,      ActingUser, All, VHost, fun add_queue/3),
                rabbit_log:info("Importing exchanges..."),
                for_all(exchanges,   ActingUser, All, VHost, fun add_exchange/3),
                rabbit_log:info("Importing bindings..."),
                for_all(bindings,    ActingUser, All, VHost, fun add_binding/3),
                SuccessFun()
            catch {error, E} -> ErrorFun(format(E));
                  exit:E     -> ErrorFun(format(E))
            end
    end.

for_all(Name, ActingUser, All, Fun) ->
    case maps:get(Name, All, undefined) of
        undefined -> ok;
        List      -> [Fun(maps:from_list([{atomise_name(K), V} || {K, V} <- maps:to_list(M)]),
                          ActingUser) ||
                         M <- List, is_map(M)]
    end.

for_all(Name, ActingUser, All, VHost, Fun) ->
    case maps:get(Name, All, undefined) of
        undefined -> ok;
        List      -> [Fun(VHost, maps:from_list([{atomise_name(K), V} || {K, V} <- maps:to_list(M)]),
                          ActingUser) ||
                         M <- List, is_map(M)]
    end.

format(#amqp_error{name = Name, explanation = Explanation}) ->
    rabbit_data_coercion:to_binary(rabbit_misc:format("~s: ~s", [Name, Explanation]));
format({no_such_vhost, undefined}) ->
    rabbit_data_coercion:to_binary(
      "Virtual host does not exist and is not specified in definitions file.");
format({no_such_vhost, VHost}) ->
    rabbit_data_coercion:to_binary(
      rabbit_misc:format("Please create virtual host \"~s\" prior to importing definitions.",
                         [VHost]));
format({vhost_limit_exceeded, ErrMsg}) ->
    rabbit_data_coercion:to_binary(ErrMsg);
format(E) ->
    rabbit_data_coercion:to_binary(rabbit_misc:format("~p", [E])).

add_parameter(Param, Username) ->
    VHost = maps:get(vhost,     Param, undefined),
    add_parameter(VHost, Param, Username).

add_parameter(VHost, Param, Username) ->
    Comp  = maps:get(component, Param, undefined),
    Key   = maps:get(name,      Param, undefined),
    Term  = maps:get(value,     Param, undefined),
    Result = case is_map(Term) of
        true ->
            %% coerce maps to proplists for backwards compatibility.
            %% See rabbitmq-management#528.
            TermProplist = rabbit_data_coercion:to_proplist(Term),
            rabbit_runtime_parameters:set(VHost, Comp, Key, TermProplist, Username);
        _ ->
            rabbit_runtime_parameters:set(VHost, Comp, Key, Term, Username)
    end,
    case Result of
        ok                -> ok;
        {error_string, E} ->
            S = rabbit_misc:format(" (~s/~s/~s)", [VHost, Comp, Key]),
            exit(rabbit_data_coercion:to_binary(rabbit_misc:escape_html_tags(E ++ S)))
    end.

add_global_parameter(Param, Username) ->
    Key   = maps:get(name,      Param, undefined),
    Term  = maps:get(value,     Param, undefined),
    case is_map(Term) of
        true ->
            %% coerce maps to proplists for backwards compatibility.
            %% See rabbitmq-management#528.
            TermProplist = rabbit_data_coercion:to_proplist(Term),
            rabbit_runtime_parameters:set_global(Key, TermProplist, Username);
        _ ->
            rabbit_runtime_parameters:set_global(Key, Term, Username)
    end.

add_policy(Param, Username) ->
    VHost = maps:get(vhost, Param, undefined),
    add_policy(VHost, Param, Username).

add_policy(VHost, Param, Username) ->
    Key   = maps:get(name,  Param, undefined),
    case rabbit_policy:set(
           VHost, Key, maps:get(pattern, Param, undefined),
           case maps:get(definition, Param, undefined) of
               undefined -> undefined;
               Def -> rabbit_data_coercion:to_proplist(Def)
           end,
           maps:get(priority, Param, undefined),
           maps:get('apply-to', Param, <<"all">>),
           Username) of
        ok                -> ok;
        {error_string, E} -> S = rabbit_misc:format(" (~s/~s)", [VHost, Key]),
                             exit(rabbit_data_coercion:to_binary(rabbit_misc:escape_html_tags(E ++ S)))
    end.

add_vhost(VHost, ActingUser) ->
    VHostName = maps:get(name, VHost, undefined),
    VHostTrace = maps:get(tracing, VHost, undefined),
    VHostDefinition = maps:get(definition, VHost, undefined),
    VHostTags = maps:get(tags, VHost, undefined),
    rabbit_vhost:put_vhost(VHostName, VHostDefinition, VHostTags, VHostTrace, ActingUser).

add_permission(Permission, ActingUser) ->
    rabbit_auth_backend_internal:set_permissions(maps:get(user,      Permission, undefined),
                                                 maps:get(vhost,     Permission, undefined),
                                                 maps:get(configure, Permission, undefined),
                                                 maps:get(write,     Permission, undefined),
                                                 maps:get(read,      Permission, undefined),
                                                 ActingUser).

add_topic_permission(TopicPermission, ActingUser) ->
    rabbit_auth_backend_internal:set_topic_permissions(
        maps:get(user,      TopicPermission, undefined),
        maps:get(vhost,     TopicPermission, undefined),
        maps:get(exchange,  TopicPermission, undefined),
        maps:get(write,     TopicPermission, undefined),
        maps:get(read,      TopicPermission, undefined),
      ActingUser).

add_queue(Queue, ActingUser) ->
    add_queue_int(Queue, r(queue, Queue), ActingUser).

add_queue(VHost, Queue, ActingUser) ->
    add_queue_int(Queue, rv(VHost, queue, Queue), ActingUser).

add_queue_int(Queue, Name, ActingUser) ->
    rabbit_amqqueue:declare(Name,
                            maps:get(durable,                         Queue, undefined),
                            maps:get(auto_delete,                     Queue, undefined),
                            args(maps:get(arguments, Queue, undefined)),
                            none,
                            ActingUser).

add_exchange(Exchange, ActingUser) ->
    add_exchange_int(Exchange, r(exchange, Exchange), ActingUser).

add_exchange(VHost, Exchange, ActingUser) ->
    add_exchange_int(Exchange, rv(VHost, exchange, Exchange), ActingUser).

add_exchange_int(Exchange, Name, ActingUser) ->
    Internal = case maps:get(internal, Exchange, undefined) of
                   undefined -> false; %% =< 2.2.0
                   I         -> I
               end,
    rabbit_exchange:declare(Name,
                            rabbit_exchange:check_type(maps:get(type, Exchange, undefined)),
                            maps:get(durable,                         Exchange, undefined),
                            maps:get(auto_delete,                     Exchange, undefined),
                            Internal,
                            args(maps:get(arguments, Exchange, undefined)),
                            ActingUser).

add_binding(Binding, ActingUser) ->
    DestType = dest_type(Binding),
    add_binding_int(Binding, r(exchange, source, Binding),
                    r(DestType, destination, Binding), ActingUser).

add_binding(VHost, Binding, ActingUser) ->
    DestType = dest_type(Binding),
    add_binding_int(Binding, rv(VHost, exchange, source, Binding),
                    rv(VHost, DestType, destination, Binding), ActingUser).

add_binding_int(Binding, Source, Destination, ActingUser) ->
    rabbit_binding:add(
      #binding{source       = Source,
               destination  = Destination,
               key          = maps:get(routing_key, Binding, undefined),
               args         = args(maps:get(arguments, Binding, undefined))},
      ActingUser).

dest_type(Binding) ->
    list_to_atom(binary_to_list(maps:get(destination_type, Binding, undefined))).

r(Type, Props) -> r(Type, name, Props).

r(Type, Name, Props) ->
    rabbit_misc:r(maps:get(vhost, Props, undefined), Type, maps:get(Name, Props, undefined)).

rv(VHost, Type, Props) -> rv(VHost, Type, name, Props).

rv(VHost, Type, Name, Props) ->
    rabbit_misc:r(VHost, Type, maps:get(Name, Props, undefined)).

%%--------------------------------------------------------------------

validate_limits(All) ->
    case maps:get(queues, All, undefined) of
        undefined -> ok;
        Queues0 ->
            {ok, VHostMap} = filter_out_existing_queues(Queues0),
            maps:fold(fun validate_vhost_limit/3, ok, VHostMap)
    end.

validate_limits(All, VHost) ->
    case maps:get(queues, All, undefined) of
        undefined -> ok;
        Queues0 ->
            Queues1 = filter_out_existing_queues(VHost, Queues0),
            AddCount = length(Queues1),
            validate_vhost_limit(VHost, AddCount, ok)
    end.

filter_out_existing_queues(Queues) ->
    build_filtered_map(Queues, maps:new()).

filter_out_existing_queues(VHost, Queues) ->
    Pred = fun(Queue) ->
               Rec = rv(VHost, queue, <<"name">>, Queue),
               case rabbit_amqqueue:lookup(Rec) of
                   {ok, _} -> false;
                   {error, not_found} -> true
               end
           end,
    lists:filter(Pred, Queues).

build_queue_data(Queue) ->
    VHost = maps:get(<<"vhost">>, Queue, undefined),
    Rec = rv(VHost, queue, <<"name">>, Queue),
    {Rec, VHost}.

build_filtered_map([], AccMap) ->
    {ok, AccMap};
build_filtered_map([Queue|Rest], AccMap0) ->
    {Rec, VHost} = build_queue_data(Queue),
    case rabbit_amqqueue:lookup(Rec) of
        {error, not_found} ->
            AccMap1 = maps_update_with(VHost, fun(V) -> V + 1 end, 1, AccMap0),
            build_filtered_map(Rest, AccMap1);
        {ok, _} ->
            build_filtered_map(Rest, AccMap0)
    end.

%% Copy of maps:with_util/3 from Erlang 20.0.1.
maps_update_with(Key,Fun,Init,Map) when is_function(Fun,1), is_map(Map) ->
    case maps:find(Key,Map) of
        {ok,Val} -> maps:update(Key,Fun(Val),Map);
        error -> maps:put(Key,Init,Map)
    end;
maps_update_with(Key,Fun,Init,Map) ->
    erlang:error(maps_error_type(Map),[Key,Fun,Init,Map]).

%% Copy of maps:error_type/1 from Erlang 20.0.1.
maps_error_type(M) when is_map(M) -> badarg;
maps_error_type(V) -> {badmap, V}.

validate_vhost_limit(VHost, AddCount, ok) ->
    WouldExceed = rabbit_vhost_limit:would_exceed_queue_limit(AddCount, VHost),
    validate_vhost_queue_limit(VHost, AddCount, WouldExceed).

validate_vhost_queue_limit(_VHost, 0, _) ->
    % Note: not adding any new queues so the upload
    % must be update-only
    ok;
validate_vhost_queue_limit(_VHost, _AddCount, false) ->
    % Note: would not exceed queue limit
    ok;
validate_vhost_queue_limit(VHost, AddCount, {true, Limit, QueueCount}) ->
    ErrFmt = "Adding ~B queue(s) to virtual host \"~s\" would exceed the limit of ~B queue(s).~n~nThis virtual host currently has ~B queue(s) defined.~n~nImport aborted!",
    ErrInfo = [AddCount, VHost, Limit, QueueCount],
    ErrMsg = rabbit_misc:format(ErrFmt, ErrInfo),
    exit({vhost_limit_exceeded, ErrMsg}).

atomise_name(N) -> rabbit_data_coercion:to_atom(N).

get_or_missing(K, L) ->
    case maps:get(K, L, undefined) of
        undefined -> {key_missing, K};
        V         -> V
    end.

args([]) -> args(#{});
args(L)  -> rabbit_misc:to_amqp_table(L).
