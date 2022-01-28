%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_definitions).
-include_lib("rabbit_common/include/rabbit.hrl").

-export([boot/0]).
%% automatic import on boot
-export([
    maybe_load_definitions/0,
    maybe_load_definitions/2,
    maybe_load_definitions_from/2,

    has_configured_definitions_to_load/0
]).
%% import
-export([import_raw/1, import_raw/2, import_parsed/1, import_parsed/2,
         import_parsed_with_hashing/1, import_parsed_with_hashing/2,
         apply_defs/2, apply_defs/3, apply_defs/4, apply_defs/5,
         should_skip_if_unchanged/0]).

-export([all_definitions/0]).
-export([
  list_users/0, list_vhosts/0, list_permissions/0, list_topic_permissions/0,
  list_runtime_parameters/0, list_global_runtime_parameters/0, list_policies/0,
  list_exchanges/0, list_queues/0, list_bindings/0,
  is_internal_parameter/1
]).
-export([decode/1, decode/2, args/1]).

-import(rabbit_misc, [pget/2, pget/3]).
-import(rabbit_data_coercion, [to_binary/1]).

%%
%% API
%%

-type definition_category() :: 'users' |
                               'vhosts' |
                               'permissions' |
                               'topic_permissions' |
                               'parameters' |
                               'global_parameters' |
                               'policies' |
                               'queues' |
                               'bindings' |
                               'exchanges'.

-type definition_object() :: #{binary() => any()}.
-type definition_list() :: [definition_object()].

-type definitions() :: #{
    definition_category() => definition_list()
}.

-export_type([definition_object/0, definition_list/0, definition_category/0, definitions/0]).

-define(IMPORT_WORK_POOL, definition_import_pool).

boot() ->
    PoolSize = application:get_env(rabbit, definition_import_work_pool_size, rabbit_runtime:guess_number_of_cpu_cores()),
    rabbit_sup:start_supervisor_child(definition_import_pool_sup, worker_pool_sup, [PoolSize, ?IMPORT_WORK_POOL]).

maybe_load_definitions() ->
    %% Classic source: local file or data directory
    maybe_load_definitions_from_local_filesystem(rabbit, load_definitions),
    %% Extensible sources
    maybe_load_definitions_from_pluggable_source(rabbit, definitions).

-spec import_raw(Body :: binary() | iolist()) -> ok | {error, term()}.
import_raw(Body) ->
    rabbit_log:info("Asked to import definitions. Acting user: ~s", [?INTERNAL_USER]),
    case decode([], Body) of
        {error, E}   -> {error, E};
        {ok, _, Map} -> apply_defs(Map, ?INTERNAL_USER)
    end.

-spec import_raw(Body :: binary() | iolist(), VHost :: vhost:name()) -> ok | {error, term()}.
import_raw(Body, VHost) ->
    rabbit_log:info("Asked to import definitions. Acting user: ~s", [?INTERNAL_USER]),
    case decode([], Body) of
        {error, E}   -> {error, E};
        {ok, _, Map} -> apply_defs(Map, ?INTERNAL_USER, fun() -> ok end, VHost)
    end.

-spec import_parsed(Defs :: #{any() => any()} | list()) -> ok | {error, term()}.
import_parsed(Body0) when is_list(Body0) ->
    import_parsed(maps:from_list(Body0));
import_parsed(Body0) when is_map(Body0) ->
    rabbit_log:info("Asked to import definitions. Acting user: ~s", [?INTERNAL_USER]),
    Body = atomise_map_keys(Body0),
    apply_defs(Body, ?INTERNAL_USER).

-spec import_parsed(Defs :: #{any() => any() | list()}, VHost :: vhost:name()) -> ok | {error, term()}.
import_parsed(Body0, VHost) when is_list(Body0) ->
    import_parsed(maps:from_list(Body0), VHost);
import_parsed(Body0, VHost) ->
    rabbit_log:info("Asked to import definitions. Acting user: ~s", [?INTERNAL_USER]),
    Body = atomise_map_keys(Body0),
    apply_defs(Body, ?INTERNAL_USER, fun() -> ok end, VHost).


-spec import_parsed_with_hashing(Defs :: #{any() => any()} | list()) -> ok | {error, term()}.
import_parsed_with_hashing(Body0) when is_list(Body0) ->
    import_parsed(maps:from_list(Body0));
import_parsed_with_hashing(Body0) when is_map(Body0) ->
    rabbit_log:info("Asked to import definitions. Acting user: ~s", [?INTERNAL_USER]),
    case should_skip_if_unchanged() of
        false ->
            import_parsed(Body0);
        true  ->
            Body         = atomise_map_keys(Body0),
            PreviousHash = rabbit_definitions_hashing:stored_global_hash(),
            Algo         = rabbit_definitions_hashing:hashing_algorithm(),
            case rabbit_definitions_hashing:hash(Algo, Body) of
                PreviousHash -> ok;
                Other        ->
                    Result = apply_defs(Body, ?INTERNAL_USER),
                    rabbit_definitions_hashing:store_global_hash(Other),
                    Result
            end
    end.

-spec import_parsed_with_hashing(Defs :: #{any() => any() | list()}, VHost :: vhost:name()) -> ok | {error, term()}.
import_parsed_with_hashing(Body0, VHost) when is_list(Body0) ->
    import_parsed(maps:from_list(Body0), VHost);
import_parsed_with_hashing(Body0, VHost) ->
    rabbit_log:info("Asked to import definitions for virtual host '~s'. Acting user: ~s", [?INTERNAL_USER, VHost]),

    case should_skip_if_unchanged() of
        false ->
            import_parsed(Body0, VHost);
        true  ->
            Body = atomise_map_keys(Body0),
            PreviousHash = rabbit_definitions_hashing:stored_vhost_specific_hash(VHost),
            Algo         = rabbit_definitions_hashing:hashing_algorithm(),
            case rabbit_definitions_hashing:hash(Algo, Body) of
                PreviousHash -> ok;
                Other        ->
                    Result = apply_defs(Body, ?INTERNAL_USER, fun() -> ok end, VHost),
                    rabbit_definitions_hashing:store_vhost_specific_hash(VHost, Other, ?INTERNAL_USER),
                    Result
            end
    end.


-spec all_definitions() -> map().
all_definitions() ->
    Xs = list_exchanges(),
    Qs = list_queues(),
    Bs = list_bindings(),

    Users  = list_users(),
    VHosts = list_vhosts(),
    Params  = list_runtime_parameters(),
    GParams = list_global_runtime_parameters(),
    Pols    = list_policies(),

    Perms   = list_permissions(),
    TPerms  = list_topic_permissions(),

    {ok, Vsn} = application:get_key(rabbit, vsn),
    #{
        rabbit_version    => rabbit_data_coercion:to_binary(Vsn),
        rabbitmq_version  => rabbit_data_coercion:to_binary(Vsn),
        users             => Users,
        vhosts            => VHosts,
        permissions       => Perms,
        topic_permissions => TPerms,
        parameters        => Params,
        global_parameters => GParams,
        policies          => Pols,
        queues            => Qs,
        bindings          => Bs,
        exchanges         => Xs
    }.

-spec has_configured_definitions_to_load() -> boolean().
has_configured_definitions_to_load() ->
    has_configured_definitions_to_load_via_classic_option() or has_configured_definitions_to_load_via_modern_option().

%% Retained for backwards compatibility, implicitly assumes the local filesystem source
maybe_load_definitions(App, Key) ->
    maybe_load_definitions_from_local_filesystem(App, Key).

maybe_load_definitions_from(IsDir, Path) ->
    rabbit_definitions_import_local_filesystem:load(IsDir, Path).

%%
%% Implementation
%%

-spec has_configured_definitions_to_load_via_modern_option() -> boolean().
has_configured_definitions_to_load_via_modern_option() ->
    case application:get_env(rabbit, definitions) of
        undefined  -> false;
        {ok, none} -> false;
        {ok, []}   -> false;
        {ok, _Options} -> true
    end.

has_configured_definitions_to_load_via_classic_option() ->
    case application:get_env(rabbit, load_definitions) of
        undefined   -> false;
        {ok, none}  -> false;
        {ok, _Path} -> true
    end.

maybe_load_definitions_from_local_filesystem(App, Key) ->
    case application:get_env(App, Key) of
        undefined  -> ok;
        {ok, none} -> ok;
        {ok, Path} ->
            IsDir = filelib:is_dir(Path),
            Mod = rabbit_definitions_import_local_filesystem,

            case should_skip_if_unchanged() of
                false ->
                    rabbit_log:debug("Will use module ~s to import definitions", [Mod]),
                    Mod:load(IsDir, Path);
                true ->
                    rabbit_log:debug("Will use module ~s to import definitions (if definition file/directory has changed)", [Mod]),
                    CurrentHash = rabbit_definitions_hashing:stored_global_hash(),
                    rabbit_log:info("Previously stored hash value of imported definitions: ~p...", [rabbit_misc:hexify(CurrentHash)]),
                    Algo = rabbit_definitions_hashing:hashing_algorithm(),
                    case Mod:load_with_hashing(IsDir, Path, CurrentHash, Algo) of
                        CurrentHash ->
                            rabbit_log:info("Hash value of imported definitions matches current contents");
                        UpdatedHash ->
                            rabbit_log:debug("Hash value of imported definitions has changed to ~p", [rabbit_misc:hexify(CurrentHash)]),
                            rabbit_definitions_hashing:store_global_hash(UpdatedHash)
                    end
            end
    end.

maybe_load_definitions_from_pluggable_source(App, Key) ->
    case application:get_env(App, Key) of
        undefined  -> ok;
        {ok, none} -> ok;
        {ok, []}   -> ok;
        {ok, Proplist} ->
            case pget(import_backend, Proplist, undefined) of
                undefined  ->
                    {error, "definition import source is configured but definitions.import_backend is not set"};
                ModOrAlias ->
                    Mod = normalize_backend_module(ModOrAlias),
                    case should_skip_if_unchanged() of
                        false ->
                            rabbit_log:debug("Will use module ~s to import definitions", [Mod]),
                            Mod:load(Proplist);
                        true ->
                            rabbit_log:debug("Will use module ~s to import definitions (if definition file/directory/source has changed)", [Mod]),
                            CurrentHash = rabbit_definitions_hashing:stored_global_hash(),
                            rabbit_log:info("Previously stored hash value of imported definitions: ~s...", [rabbit_misc:hexify(CurrentHash)]),
                            Algo = rabbit_definitions_hashing:hashing_algorithm(),
                            case Mod:load_with_hashing(Proplist, CurrentHash, Algo) of
                                CurrentHash ->
                                    rabbit_log:info("Hash value of imported definitions matches current contents");
                                UpdatedHash ->
                                    rabbit_log:debug("Hash value of imported definitions has changed to ~s...", [rabbit_misc:hexify(CurrentHash)]),
                                    rabbit_definitions_hashing:store_global_hash(UpdatedHash)
                            end
                    end
            end
    end.

normalize_backend_module(local_filesystem) ->
    rabbit_definitions_import_local_filesystem;
normalize_backend_module(local) ->
    rabbit_definitions_import_local_filesystem;
normalize_backend_module(https) ->
    rabbit_definitions_import_https;
normalize_backend_module(http)  ->
    rabbit_definitions_import_https;
normalize_backend_module(rabbitmq_definitions_import_local_filesystem) ->
    rabbit_definitions_import_local_filesystem;
normalize_backend_module(rabbitmq_definitions_import_https) ->
    rabbit_definitions_import_https;
normalize_backend_module(Other) ->
    Other.

decode(Keys, Body) ->
    case decode(Body) of
        {ok, J0} ->
            J = maps:fold(fun(K, V, Acc) ->
                  Acc#{rabbit_data_coercion:to_atom(K, utf8) => V}
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
      Decoded = rabbit_json:decode(Body),
      Normalised = atomise_map_keys(Decoded),
      {ok, Normalised}
    catch error:_ -> {error, not_json}
    end.

atomise_map_keys(Decoded) ->
    maps:fold(fun(K, V, Acc) ->
        Acc#{rabbit_data_coercion:to_atom(K, utf8) => V}
              end, Decoded, Decoded).

-spec should_skip_if_unchanged() -> boolean().
should_skip_if_unchanged() ->
    case application:get_env(rabbit, definitions) of
        undefined   -> false;
        {ok, none}  -> false;
        {ok, []}    -> false;
        {ok, Proplist} ->
            pget(skip_if_unchanged, Proplist, false)
    end.


-spec apply_defs(Map :: #{atom() => any()}, ActingUser :: rabbit_types:username()) -> 'ok' | {error, term()}.

apply_defs(Map, ActingUser) ->
    apply_defs(Map, ActingUser, fun () -> ok end).

-spec apply_defs(Map :: #{atom() => any()}, ActingUser :: rabbit_types:username(),
                SuccessFun :: fun(() -> 'ok')) -> 'ok'  | {error, term()};
                (Map :: #{atom() => any()}, ActingUser :: rabbit_types:username(),
                VHost :: vhost:name()) -> 'ok'  | {error, term()}.

apply_defs(Map, ActingUser, SuccessFun) when is_function(SuccessFun) ->
    Version = maps:get(rabbitmq_version, Map, maps:get(rabbit_version, Map, undefined)),
    try
        concurrent_for_all(users, ActingUser, Map,
                fun(User, _Username) ->
                    rabbit_auth_backend_internal:put_user(User, Version, ActingUser)
                end),
        concurrent_for_all(vhosts,             ActingUser, Map, fun add_vhost/2),
        validate_limits(Map),
        concurrent_for_all(permissions,        ActingUser, Map, fun add_permission/2),
        concurrent_for_all(topic_permissions,  ActingUser, Map, fun add_topic_permission/2),

        concurrent_for_all(exchanges,          ActingUser, Map, fun add_exchange/2),

        sequential_for_all(global_parameters,  ActingUser, Map, fun add_global_parameter/2),
        %% importing policies concurrently can be unsafe as queues will be getting
        %% potentially out of order notifications of applicable policy changes
        sequential_for_all(policies,           ActingUser, Map, fun add_policy/2),
        sequential_for_all(parameters,         ActingUser, Map, fun add_parameter/2),

        rabbit_nodes:if_reached_target_cluster_size(
            fun() ->
                concurrent_for_all(queues,   ActingUser, Map, fun add_queue/2),
                concurrent_for_all(bindings, ActingUser, Map, fun add_binding/2)
            end,

            fun() ->
                rabbit_log:info("There are fewer than target cluster size (~b) nodes online,"
                                " skipping queue and binding import from definitions",
                                [rabbit_nodes:target_cluster_size_hint()])
            end
        ),

        SuccessFun(),
        ok
    catch {error, E} -> {error, E};
          exit:E     -> {error, E}
    end.

-spec apply_defs(Map :: #{atom() => any()},
                ActingUser :: rabbit_types:username(),
                SuccessFun :: fun(() -> 'ok'),
                VHost :: vhost:name()) -> 'ok' | {error, term()}.

apply_defs(Map, ActingUser, SuccessFun, VHost) when is_binary(VHost) ->
    rabbit_log:info("Asked to import definitions for a virtual host. Virtual host: ~p, acting user: ~p",
                    [VHost, ActingUser]),
    try
        validate_limits(Map, VHost),

        concurrent_for_all(exchanges,  ActingUser, Map, VHost, fun add_exchange/3),
        sequential_for_all(parameters, ActingUser, Map, VHost, fun add_parameter/3),
        %% importing policies concurrently can be unsafe as queues will be getting
        %% potentially out of order notifications of applicable policy changes
        sequential_for_all(policies,   ActingUser, Map, VHost, fun add_policy/3),

        rabbit_nodes:if_reached_target_cluster_size(
            fun() ->
                concurrent_for_all(queues,     ActingUser, Map, VHost, fun add_queue/3),
                concurrent_for_all(bindings,   ActingUser, Map, VHost, fun add_binding/3)
            end,

            fun() ->
                rabbit_log:info("There are fewer than target cluster size (~b) nodes online,"
                                " skipping queue and binding import from definitions",
                                [rabbit_nodes:target_cluster_size_hint()])
            end
        ),

        SuccessFun()
    catch {error, E} -> {error, format(E)};
          exit:E     -> {error, format(E)}
    end.

-spec apply_defs(Map :: #{atom() => any()},
                ActingUser :: rabbit_types:username(),
                SuccessFun :: fun(() -> 'ok'),
                ErrorFun :: fun((any()) -> 'ok'),
                VHost :: vhost:name()) -> 'ok' | {error, term()}.

apply_defs(Map, ActingUser, SuccessFun, ErrorFun, VHost) ->
    rabbit_log:info("Asked to import definitions for a virtual host. Virtual host: ~p, acting user: ~p",
                    [VHost, ActingUser]),
    try
        validate_limits(Map, VHost),

        concurrent_for_all(bindings,   ActingUser, Map, VHost, fun add_binding/3),
        sequential_for_all(parameters, ActingUser, Map, VHost, fun add_parameter/3),
        %% importing policies concurrently can be unsafe as queues will be getting
        %% potentially out of order notifications of applicable policy changes
        sequential_for_all(policies,   ActingUser, Map, VHost, fun add_policy/3),

        rabbit_nodes:if_reached_target_cluster_size(
            fun() ->
                concurrent_for_all(queues,     ActingUser, Map, VHost, fun add_queue/3),
                concurrent_for_all(bindings,   ActingUser, Map, VHost, fun add_binding/3)
            end,

            fun() ->
                rabbit_log:info("There are fewer than target cluster size (~b) nodes online,"
                                " skipping queue and binding import from definitions",
                                [rabbit_nodes:target_cluster_size_hint()])
            end
        ),

        SuccessFun()
    catch {error, E} -> ErrorFun(format(E));
          exit:E     -> ErrorFun(format(E))
    end.

sequential_for_all(Category, ActingUser, Definitions, Fun) ->
    case maps:get(rabbit_data_coercion:to_atom(Category), Definitions, undefined) of
        undefined -> ok;
        List      ->
            case length(List) of
                0 -> ok;
                N -> rabbit_log:info("Importing sequentially ~p ~s...", [N, human_readable_category_name(Category)])
            end,
            [begin
                 %% keys are expected to be atoms
                 Fun(atomize_keys(M), ActingUser)
             end || M <- List, is_map(M)]
    end.

sequential_for_all(Name, ActingUser, Definitions, VHost, Fun) ->
    case maps:get(rabbit_data_coercion:to_atom(Name), Definitions, undefined) of
        undefined -> ok;
        List      -> [Fun(VHost, atomize_keys(M), ActingUser) || M <- List, is_map(M)]
    end.

concurrent_for_all(Category, ActingUser, Definitions, Fun) ->
    case maps:get(rabbit_data_coercion:to_atom(Category), Definitions, undefined) of
        undefined -> ok;
        List      ->
            case length(List) of
                0 -> ok;
                N -> rabbit_log:info("Importing concurrently ~p ~s...", [N, human_readable_category_name(Category)])
            end,
            WorkPoolFun = fun(M) ->
                                  Fun(atomize_keys(M), ActingUser)
                          end,
            do_concurrent_for_all(List, WorkPoolFun)
    end.

concurrent_for_all(Name, ActingUser, Definitions, VHost, Fun) ->
    case maps:get(rabbit_data_coercion:to_atom(Name), Definitions, undefined) of
        undefined -> ok;
        List      ->
            WorkPoolFun = fun(M) ->
                                  Fun(VHost, atomize_keys(M), ActingUser)
                          end,
            do_concurrent_for_all(List, WorkPoolFun)
    end.

do_concurrent_for_all(List, WorkPoolFun) ->
    {ok, Gatherer} = gatherer:start_link(),
    [begin
         %% keys are expected to be atoms
         ok = gatherer:fork(Gatherer),
         worker_pool:submit_async(
           ?IMPORT_WORK_POOL,
           fun() ->
                   try
                       WorkPoolFun(M)
                   catch {error, E} -> gatherer:in(Gatherer, {error, E});
                         _:E        -> gatherer:in(Gatherer, {error, E})
                   end,
                   gatherer:finish(Gatherer)
           end)
     end || M <- List, is_map(M)],
    case gatherer:out(Gatherer) of
        empty ->
            ok = gatherer:stop(Gatherer);
        {value, {error, E}} ->
            ok = gatherer:stop(Gatherer),
            throw({error, E})
    end.

-spec atomize_keys(#{any() => any()}) -> #{atom() => any()}.

atomize_keys(M) ->
    maps:fold(fun(K, V, Acc) ->
                maps:put(rabbit_data_coercion:to_atom(K), V, Acc)
              end, #{}, M).

-spec human_readable_category_name(definition_category()) -> string().

human_readable_category_name(topic_permissions) -> "topic permissions";
human_readable_category_name(parameters) -> "runtime parameters";
human_readable_category_name(global_parameters) -> "global runtime parameters";
human_readable_category_name(Other) -> rabbit_data_coercion:to_list(Other).


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
    case Key of
      undefined -> exit(rabbit_misc:format("policy in virtual host '~s' has undefined name", [VHost]));
      _ -> ok
    end,
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

-spec add_vhost(map(), rabbit_types:username()) -> ok.

add_vhost(VHost, ActingUser) ->
    Name             = maps:get(name, VHost, undefined),
    IsTracingEnabled = maps:get(tracing, VHost, undefined),
    Metadata         = rabbit_data_coercion:atomize_keys(maps:get(metadata, VHost, #{})),
    Description      = maps:get(description, VHost, maps:get(description, Metadata, <<"">>)),
    Tags             = maps:get(tags, VHost, maps:get(tags, Metadata, [])),

    rabbit_vhost:put_vhost(Name, Description, Tags, IsTracingEnabled, ActingUser).

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

add_queue_int(_Queue, R = #resource{kind = queue,
                                    name = <<"amq.", _/binary>>}, ActingUser) ->
    Name = R#resource.name,
    rabbit_log:warning("Skipping import of a queue whose name begins with 'amq.', "
                       "name: ~s, acting user: ~s", [Name, ActingUser]);
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

add_exchange_int(_Exchange, #resource{kind = exchange, name = <<"">>}, ActingUser) ->
    rabbit_log:warning("Not importing the default exchange, acting user: ~s", [ActingUser]);
add_exchange_int(_Exchange, R = #resource{kind = exchange,
                                          name = <<"amq.", _/binary>>}, ActingUser) ->
    Name = R#resource.name,
    rabbit_log:warning("Skipping import of an exchange whose name begins with 'amq.', "
                       "name: ~s, acting user: ~s", [Name, ActingUser]);
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
    rabbit_data_coercion:to_atom(maps:get(destination_type, Binding, undefined)).

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
            AccMap1 = maps:update_with(VHost, fun(V) -> V + 1 end, 1, AccMap0),
            build_filtered_map(Rest, AccMap1);
        {ok, _} ->
            build_filtered_map(Rest, AccMap0)
    end.

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

get_or_missing(K, L) ->
    case maps:get(K, L, undefined) of
        undefined -> {key_missing, K};
        V         -> V
    end.

args(undefined) -> args(#{});
args([]) -> args(#{});
args(L)  -> rabbit_misc:to_amqp_table(L).

%%
%% Export
%%

list_exchanges() ->
    %% exclude internal exchanges, they are not meant to be declared or used by
    %% applications
    [exchange_definition(X) || X <- lists:filter(fun(#exchange{internal = true}) -> false;
                                                    (#exchange{name = #resource{name = <<>>}}) -> false;
                                                    (X) -> not rabbit_exchange:is_amq_prefixed(X)
                                                 end,
                                                 rabbit_exchange:list())].

exchange_definition(#exchange{name = #resource{virtual_host = VHost, name = Name},
                              type = Type,
                              durable = Durable, auto_delete = AD, arguments = Args}) ->
    #{<<"vhost">> => VHost,
      <<"name">> => Name,
      <<"type">> => Type,
      <<"durable">> => Durable,
      <<"auto_delete">> => AD,
      <<"arguments">> => rabbit_misc:amqp_table(Args)}.

list_queues() ->
    %% exclude exclusive queues, they cannot be restored
    [queue_definition(Q) || Q <- lists:filter(fun(Q0) ->
                                                amqqueue:get_exclusive_owner(Q0) =:= none
                                              end,
                                              rabbit_amqqueue:list())].

queue_definition(Q) ->
    #resource{virtual_host = VHost, name = Name} = amqqueue:get_name(Q),
    Type = case amqqueue:get_type(Q) of
               rabbit_classic_queue -> classic;
               rabbit_quorum_queue -> quorum;
               rabbit_stream_queue -> stream;
               T -> T
           end,
    #{
        <<"vhost">> => VHost,
        <<"name">> => Name,
        <<"type">> => Type,
        <<"durable">> => amqqueue:is_durable(Q),
        <<"auto_delete">> => amqqueue:is_auto_delete(Q),
        <<"arguments">> => rabbit_misc:amqp_table(amqqueue:get_arguments(Q))
    }.

list_bindings() ->
    [binding_definition(B) || B <- rabbit_binding:list_explicit()].

binding_definition(#binding{source      = S,
                            key         = RoutingKey,
                            destination = D,
                            args        = Args}) ->
    #{
        <<"source">> => S#resource.name,
        <<"vhost">> => S#resource.virtual_host,
        <<"destination">> => D#resource.name,
        <<"destination_type">> => D#resource.kind,
        <<"routing_key">> => RoutingKey,
        <<"arguments">> => rabbit_misc:amqp_table(Args)
    }.

list_vhosts() ->
    [vhost_definition(V) || V <- rabbit_vhost:all()].

vhost_definition(VHost) ->
    #{
        <<"name">> => vhost:get_name(VHost),
        <<"limits">> => vhost:get_limits(VHost),
        <<"metadata">> => vhost:get_metadata(VHost)
    }.

list_users() ->
    [user_definition(U) || U <- rabbit_auth_backend_internal:all_users()].

user_definition(User) ->
    #{<<"name">>              => internal_user:get_username(User),
      <<"password_hash">>     => base64:encode(internal_user:get_password_hash(User)),
      <<"hashing_algorithm">> => rabbit_auth_backend_internal:hashing_module_for_user(User),
      <<"tags">>              => tags_as_binaries(internal_user:get_tags(User)),
      <<"limits">>            => internal_user:get_limits(User)
    }.

list_runtime_parameters() ->
    [runtime_parameter_definition(P) || P <- rabbit_runtime_parameters:list(), is_list(P)].

runtime_parameter_definition(Param) ->
    #{
        <<"vhost">> => pget(vhost, Param),
        <<"component">> => pget(component, Param),
        <<"name">> => pget(name, Param),
        <<"value">> => maps:from_list(pget(value, Param))
    }.

list_global_runtime_parameters() ->
    [global_runtime_parameter_definition(P) || P <- rabbit_runtime_parameters:list_global(), not is_internal_parameter(P)].

global_runtime_parameter_definition(P0) ->
    P = [{rabbit_data_coercion:to_binary(K), V} || {K, V} <- P0],
    maps:from_list(P).

-define(INTERNAL_GLOBAL_PARAM_PREFIX, "internal").

is_internal_parameter(Param) ->
    Name = rabbit_data_coercion:to_list(pget(name, Param)),
    %% if global parameter name starts with an "internal", consider it to be internal
    %% and exclude it from definition export
    string:left(Name, length(?INTERNAL_GLOBAL_PARAM_PREFIX)) =:= ?INTERNAL_GLOBAL_PARAM_PREFIX.

list_policies() ->
    [policy_definition(P) || P <- rabbit_policy:list()].

policy_definition(Policy) ->
    #{
        <<"vhost">> => pget(vhost, Policy),
        <<"name">> => pget(name, Policy),
        <<"pattern">> => pget(pattern, Policy),
        <<"apply-to">> => pget('apply-to', Policy),
        <<"priority">> => pget(priority, Policy),
        <<"definition">> => maps:from_list(pget(definition, Policy))
    }.

list_permissions() ->
    [permission_definition(P) || P <- rabbit_auth_backend_internal:list_permissions()].

permission_definition(P0) ->
    P = [{rabbit_data_coercion:to_binary(K), V} || {K, V} <- P0],
    maps:from_list(P).

list_topic_permissions() ->
    [topic_permission_definition(P) || P <- rabbit_auth_backend_internal:list_topic_permissions()].

topic_permission_definition(P0) ->
    P = [{rabbit_data_coercion:to_binary(K), V} || {K, V} <- P0],
    maps:from_list(P).

tags_as_binaries(Tags) ->
    [to_binary(T) || T <- Tags].
