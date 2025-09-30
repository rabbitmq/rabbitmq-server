%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_parameters).
-behaviour(rabbit_runtime_parameter).

-define(APP, rabbitmq_shovel).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").
-include_lib("kernel/include/logger.hrl").

-export([validate/5, notify/5, notify_clear/4]).
-export([register/0, unregister/0, parse/3]).
-export([obfuscate_uris_in_definition/1]).

%% Function references should not be stored on the metadata store.
%% They are only valid for the version of the module they were created
%% from and can break with the next upgrade. It should not be used by
%% another one that the one who created it or survive a node restart.
%% Thus, function references have been replace by the following MFA.
-export([dest_decl/4, dest_check/4,
         src_decl_exchange/4, src_decl_queue/4, src_check_queue/4,
         fields_fun/5, props_fun/9]).

-export([is_internal/1, internal_owner/1]).

-import(rabbit_misc, [pget/2, pget/3, pset/3]).

-rabbit_boot_step({?MODULE,
                   [{description, "shovel parameters"},
                    {mfa, {rabbit_shovel_parameters, register, []}},
                    {cleanup, {?MODULE, unregister, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    OpMode = rabbit_shovel_operating_mode:operating_mode(),
    case OpMode of
        standard ->
            rabbit_registry:register(runtime_parameter, <<"shovel">>, ?MODULE);
        _Other ->
            ?LOG_DEBUG("Shovel: skipping runtime parameter registration, operating mode: ~ts", [OpMode])
    end.

unregister() ->
    OpMode = rabbit_shovel_operating_mode:operating_mode(),
    case OpMode of
        standard ->
            rabbit_registry:unregister(runtime_parameter, <<"shovel">>);
        _Other ->
            ?LOG_DEBUG("Shovel: skipping runtime parameter deregistration, operating mode: ~ts", [OpMode])
    end.

validate(_VHost, <<"shovel">>, Name, Def0, User) ->
    Def = rabbit_data_coercion:to_proplist(Def0),
    Validations =
        shovel_validation()
        ++ src_validation(Def, User)
        ++ dest_validation(Def, User),
    validate_src(Def)
    ++ validate_dest(Def)
    ++ rabbit_parameter_validation:proplist(Name, Validations, Def);

validate(_VHost, _Component, Name, _Term, _User) ->
    {error, "name not recognised: ~tp", [Name]}.

pget2(K1, K2, Defs) -> case {pget(K1, Defs), pget(K2, Defs)} of
                           {undefined, undefined} -> zero;
                           {undefined, _}         -> one;
                           {_,         undefined} -> one;
                           {_,         _}         -> both
                       end.

notify(VHost, <<"shovel">>, Name, Definition, _Username) ->
    OpMode = rabbit_shovel_operating_mode:operating_mode(),
    case OpMode of
        standard ->
            rabbit_shovel_dyn_worker_sup_sup:adjust({VHost, Name}, Definition);
        _Other ->
            ?LOG_DEBUG("Shovel: ignoring a runtime parameter update, operating mode: ~ts", [OpMode])
    end.

notify_clear(VHost, <<"shovel">>, Name, _Username) ->
    OpMode = rabbit_shovel_operating_mode:operating_mode(),
    case OpMode of
        standard ->
            rabbit_shovel_dyn_worker_sup_sup:stop_child({VHost, Name}),
            %% Only necessary for shovels stuck in a restart loop, as no
            %% process is running the terminate won't be called
            rabbit_shovel_status:remove({VHost, Name});
        _Other ->
            ?LOG_DEBUG("Shovel: ignoring a cleared runtime parameter, operating mode: ~ts", [OpMode])
    end.

%%----------------------------------------------------------------------------

is_internal(Def) ->
    pget(internal, Def, pget(<<"internal">>, Def, false)).

internal_owner(Def) ->
    case pget(<<"internal_owner">>, Def, undefined) of
        undefined -> undefined;
        Owner -> rabbit_misc:r(pget(<<"virtual_host">>, Owner),
                               binary_to_existing_atom(pget(<<"kind">>, Owner)),
                               pget(<<"name">>, Owner))
    end.

validate_src(Def) ->
    case protocols(Def)  of
        {amqp091, _} -> validate_amqp091_src(Def);
        {amqp10, _} -> [];
        {local, _} -> validate_local_src(Def)
    end.

validate_dest(Def) ->
    case protocols(Def)  of
        {_, amqp091} -> validate_amqp091_dest(Def);
        {_, amqp10} -> [];
        {_, local} -> validate_local_dest(Def)
    end.

validate_amqp091_src(Def) ->
    [case pget2(<<"src-exchange">>, <<"src-queue">>, Def) of
         zero -> {error, "Must specify 'src-exchange' or 'src-queue'", []};
         one  -> ok;
         both -> {error, "Cannot specify 'src-exchange' and 'src-queue'", []}
     end,
     case {pget(<<"src-delete-after">>, Def, pget(<<"delete-after">>, Def)), pget(<<"ack-mode">>, Def)} of
         {N, <<"no-ack">>} when is_integer(N) ->
             {error, "Cannot specify 'no-ack' and numerical 'delete-after'", []};
         _ ->
             ok
     end].

validate_local_src(Def) ->
    [case pget2(<<"src-exchange">>, <<"src-queue">>, Def) of
         zero -> {error, "Must specify 'src-exchange' or 'src-queue'", []};
         one  -> ok;
         both -> {error, "Cannot specify 'src-exchange' and 'src-queue'", []}
     end,
     case {pget(<<"src-delete-after">>, Def, pget(<<"delete-after">>, Def)), pget(<<"ack-mode">>, Def)} of
         {N, <<"no-ack">>} when is_integer(N) ->
             {error, "Cannot specify 'no-ack' and numerical 'delete-after'", []};
         _ ->
             ok
     end].

obfuscate_uris_in_definition(Def) ->
  SrcURIs  = get_uris(<<"src-uri">>, Def),
  ObfuscatedSrcURIsDef = pset(<<"src-uri">>, obfuscate_uris(SrcURIs), Def),
  DestURIs  = get_uris(<<"dest-uri">>, Def),
  ObfuscatedDef = pset(<<"dest-uri">>, obfuscate_uris(DestURIs), ObfuscatedSrcURIsDef),
  ObfuscatedDef.

obfuscate_uris(URIs) ->
  [credentials_obfuscation:encrypt(URI) || URI <- URIs].

validate_amqp091_dest(Def) ->
    [case pget2(<<"dest-exchange">>, <<"dest-queue">>, Def) of
         zero -> ok;
         one  -> ok;
         both -> {error, "Cannot specify 'dest-exchange' and 'dest-queue'", []}
     end].

validate_local_dest(Def) ->
    [case pget2(<<"dest-exchange">>, <<"dest-queue">>, Def) of
         zero -> ok;
         one  -> ok;
         both -> {error, "Cannot specify 'dest-exchange' and 'dest-queue'", []}
     end].

shovel_validation() ->
    [{<<"internal">>, fun rabbit_parameter_validation:boolean/2, optional},
     {<<"internal_owner">>, fun validate_internal_owner/2, optional},
     {<<"reconnect-delay">>, fun rabbit_parameter_validation:number/2,optional},
     {<<"ack-mode">>, rabbit_parameter_validation:enum(
                        ['no-ack', 'on-publish', 'on-confirm']), optional},
     {<<"src-protocol">>,
      rabbit_parameter_validation:enum(['amqp10', 'amqp091', 'local']), optional},
     {<<"dest-protocol">>,
      rabbit_parameter_validation:enum(['amqp10', 'amqp091', 'local']), optional}
    ].

src_validation(Def, User) ->
    case protocols(Def)  of
        {amqp091, _} -> amqp091_src_validation(Def, User);
        {amqp10, _} -> amqp10_src_validation(Def, User);
        {local, _} -> local_src_validation(Def, User)
    end.

local_src_validation(_Def, User) ->
    [
     {<<"src-uri">>, validate_uri_fun(User), mandatory},
     {<<"src-exchange">>,     fun rabbit_parameter_validation:binary/2, optional},
     {<<"src-exchange-key">>, fun rabbit_parameter_validation:binary/2, optional},
     {<<"src-queue">>, fun rabbit_parameter_validation:binary/2, optional},
     {<<"src-queue-args">>,   fun validate_queue_args/2, optional},
     {<<"src-consumer-args">>, fun validate_consumer_args/2, optional},
     {<<"src-delete-after">>, fun validate_delete_after/2, optional},
     {<<"src-predeclared">>,  fun rabbit_parameter_validation:boolean/2, optional}
    ].

amqp10_src_validation(_Def, User) ->
    [
     {<<"src-uri">>, validate_uri_fun(User), mandatory},
     {<<"src-address">>, fun rabbit_parameter_validation:binary/2, mandatory},
     {<<"src-prefetch-count">>, fun rabbit_parameter_validation:number/2, optional},
     {<<"src-delete-after">>, fun validate_amqp10_delete_after/2, optional}
    ].

amqp091_src_validation(_Def, User) ->
    [
     {<<"src-uri">>,          validate_uri_fun(User), mandatory},
     {<<"src-exchange">>,     fun rabbit_parameter_validation:binary/2, optional},
     {<<"src-exchange-key">>, fun rabbit_parameter_validation:binary/2, optional},
     {<<"src-queue">>,        fun rabbit_parameter_validation:binary/2, optional},
     {<<"src-queue-args">>,   fun validate_queue_args/2, optional},
     {<<"src-consumer-args">>, fun validate_consumer_args/2, optional},
     {<<"prefetch-count">>,   fun rabbit_parameter_validation:number/2, optional},
     {<<"src-prefetch-count">>, fun rabbit_parameter_validation:number/2, optional},
     %% a deprecated pre-3.7 setting
     {<<"delete-after">>, fun validate_delete_after/2, optional},
     %% currently used multi-protocol friend name, introduced in 3.7
     {<<"src-delete-after">>, fun validate_delete_after/2, optional},
     {<<"src-predeclared">>,  fun rabbit_parameter_validation:boolean/2, optional}
    ].

dest_validation(Def0, User) ->
    Def = rabbit_data_coercion:to_proplist(Def0),
    case protocols(Def)  of
        {_, amqp091} -> amqp091_dest_validation(Def, User);
        {_, amqp10} -> amqp10_dest_validation(Def, User);
        {_, local} -> local_dest_validation(Def, User)
    end.

amqp10_dest_validation(_Def, User) ->
    [{<<"dest-uri">>, validate_uri_fun(User), mandatory},
     {<<"dest-address">>, fun rabbit_parameter_validation:binary/2, mandatory},
     {<<"dest-add-forward-headers">>, fun rabbit_parameter_validation:boolean/2, optional},
     {<<"dest-add-timestamp-header">>, fun rabbit_parameter_validation:boolean/2, optional},
     %% The bare message should be inmutable in the AMQP network.
     %% Before RabbitMQ 4.2, we allowed to set application properties, message
     %% annotations and any property. This is wrong.
     %% From 4.2, the only message modification allowed is the optional
     %% addition of forward headers and shovelled timestamp inside message
     %% annotations.
     %% To avoid breaking existing deployments, the following configuration
     %% keys are still accepted but will be ignored.
     {<<"dest-application-properties">>, fun validate_amqp10_map/2, optional},
     {<<"dest-message-annotations">>, fun validate_amqp10_map/2, optional},
     {<<"dest-properties">>, fun validate_amqp10_map/2, optional}
    ].

amqp091_dest_validation(_Def, User) ->
    [{<<"dest-uri">>,        validate_uri_fun(User), mandatory},
     {<<"dest-exchange">>,   fun rabbit_parameter_validation:binary/2,optional},
     {<<"dest-exchange-key">>,fun rabbit_parameter_validation:binary/2,optional},
     {<<"dest-queue">>,      fun rabbit_parameter_validation:amqp091_queue_name/2,optional},
     {<<"dest-queue-args">>, fun validate_queue_args/2, optional},
     {<<"add-forward-headers">>, fun rabbit_parameter_validation:boolean/2,optional},
     {<<"add-timestamp-header">>, fun rabbit_parameter_validation:boolean/2,optional},
     {<<"dest-add-forward-headers">>, fun rabbit_parameter_validation:boolean/2,optional},
     {<<"dest-add-timestamp-header">>, fun rabbit_parameter_validation:boolean/2,optional},
     {<<"publish-properties">>, fun validate_properties/2,  optional},
     {<<"dest-publish-properties">>, fun validate_properties/2,  optional},
     {<<"dest-predeclared">>,  fun rabbit_parameter_validation:boolean/2, optional}
    ].

local_dest_validation(_Def, User) ->
    [{<<"dest-uri">>,        validate_uri_fun(User), mandatory},
     {<<"dest-exchange">>,   fun rabbit_parameter_validation:binary/2,optional},
     {<<"dest-exchange-key">>,fun rabbit_parameter_validation:binary/2,optional},
     {<<"dest-queue">>,      fun rabbit_parameter_validation:amqp091_queue_name/2,optional},
     {<<"dest-queue-args">>, fun validate_queue_args/2, optional},
     {<<"dest-add-forward-headers">>, fun rabbit_parameter_validation:boolean/2,optional},
     {<<"dest-add-timestamp-header">>, fun rabbit_parameter_validation:boolean/2,optional},
     {<<"dest-predeclared">>,  fun rabbit_parameter_validation:boolean/2, optional}
    ].

validate_uri_fun(User) ->
    fun (Name, Term) -> validate_uri(Name, Term, User) end.

validate_uri(Name, Term, User) when is_binary(Term) ->
    case rabbit_parameter_validation:binary(Name, Term) of
        ok -> case amqp_uri:parse(binary_to_list(Term)) of
                  {ok, P}    -> validate_params_user(P, User);
                  {error, E} -> {error, "\"~ts\" not a valid URI: ~tp", [Term, E]}
              end;
        E  -> E
    end;
validate_uri(Name, Term, User) ->
    case rabbit_parameter_validation:list(Name, Term) of
        ok -> case [V || URI <- Term,
                         V <- [validate_uri(Name, URI, User)],
                         element(1, V) =:= error] of
                  []      -> ok;
                  [E | _] -> E
              end;
        E  -> E
    end.

validate_params_user(#amqp_params_direct{}, none) ->
    ok;
validate_params_user(#amqp_params_direct{virtual_host = VHost},
                     User = #user{username = Username}) ->
    VHostAccess = case catch rabbit_access_control:check_vhost_access(User, VHost, undefined, #{}) of
                      ok -> ok;
                      NotOK ->
                          ?LOG_DEBUG("rabbit_access_control:check_vhost_access result: ~tp", [NotOK]),
                          NotOK
                  end,
    case rabbit_vhost:exists(VHost) andalso VHostAccess of
        ok -> ok;
        _ ->
            {error, "user \"~ts\" may not connect to vhost \"~ts\"", [Username, VHost]}
    end;
validate_params_user(#amqp_params_network{}, _User) ->
    ok.

validate_delete_after(_Name, <<"never">>)          -> ok;
validate_delete_after(_Name, <<"queue-length">>)   -> ok;
validate_delete_after(_Name, N) when is_integer(N), N >= 0 -> ok;
validate_delete_after(Name,  Term) ->
    {error, "~ts should be a number greater than or equal to 0, \"never\" or \"queue-length\", actually was "
     "~tp", [Name, Term]}.

validate_amqp10_delete_after(_Name, <<"never">>)          -> ok;
validate_amqp10_delete_after(_Name, N) when is_integer(N), N >= 0 -> ok;
validate_amqp10_delete_after(Name,  Term) ->
    {error, "~ts should be a number greater than or equal to 0 or \"never\", actually was "
     "~tp", [Name, Term]}.

validate_internal_owner(Name, Term0) ->
    Term = rabbit_data_coercion:to_proplist(Term0),

    rabbit_parameter_validation:proplist(Name, [{<<"name">>, fun rabbit_parameter_validation:binary/2},
                                                {<<"kind">>, rabbit_parameter_validation:enum(
                                                              ['exchange', 'queue'])},
                                                {<<"virtual_host">>, fun rabbit_parameter_validation:binary/2}], Term).

validate_queue_args(Name, Term0) ->
    Term = rabbit_data_coercion:to_proplist(Term0),

    rabbit_parameter_validation:proplist(Name, rabbit_amqqueue:declare_args(), Term).

validate_consumer_args(Name, Term0) ->
    Term = rabbit_data_coercion:to_proplist(Term0),

    rabbit_parameter_validation:proplist(Name, rabbit_amqqueue:consume_args(), Term).

validate_amqp10_map(Name, Terms0) ->
    Terms = rabbit_data_coercion:to_proplist(Terms0),
    Str = fun rabbit_parameter_validation:binary/2,
    Validation = [{K, Str, optional} || {K, _} <- Terms],
    rabbit_parameter_validation:proplist(Name, Validation, Terms).

%% TODO headers?
validate_properties(Name, Term0) ->
    Term = case Term0 of
               T when is_map(T)  ->
                   rabbit_data_coercion:to_proplist(Term0);
               T when is_list(T) ->
                   rabbit_data_coercion:to_proplist(Term0);
               Other -> Other
           end,
    Str = fun rabbit_parameter_validation:binary/2,
    Num = fun rabbit_parameter_validation:number/2,
    rabbit_parameter_validation:proplist(
      Name, [{<<"content_type">>,     Str, optional},
             {<<"content_encoding">>, Str, optional},
             {<<"delivery_mode">>,    Num, optional},
             {<<"priority">>,         Num, optional},
             {<<"correlation_id">>,   Str, optional},
             {<<"reply_to">>,         Str, optional},
             {<<"expiration">>,       Str, optional},
             {<<"message_id">>,       Str, optional},
             {<<"timestamp">>,        Num, optional},
             {<<"type">>,             Str, optional},
             {<<"user_id">>,          Str, optional},
             {<<"app_id">>,           Str, optional},
             {<<"cluster_id">>,       Str, optional}], Term).

%%----------------------------------------------------------------------------

parse({VHost, Name}, ClusterName, Def) ->
    {Source, SourceHeaders} = parse_source(Def),
    {ok, #{name => Name,
           shovel_type => dynamic,
           source => Source,
           dest => parse_dest({VHost, Name}, ClusterName, Def,
                                      SourceHeaders),
           ack_mode => translate_ack_mode(pget(<<"ack-mode">>, Def, <<"on-confirm">>)),
           reconnect_delay => pget(<<"reconnect-delay">>, Def,
                                   ?DEFAULT_RECONNECT_DELAY)}}.

parse_source(Def) ->
    case protocols(Def) of
        {amqp10, _} -> parse_amqp10_source(Def);
        {amqp091, _} -> parse_amqp091_source(Def);
        {local, _} -> parse_local_source(Def)
    end.

parse_dest(VHostName, ClusterName, Def, SourceHeaders) ->
    case protocols(Def) of
        {_, amqp10} ->
            parse_amqp10_dest(VHostName, ClusterName, Def, SourceHeaders);
        {_, amqp091} ->
            parse_amqp091_dest(VHostName, ClusterName, Def, SourceHeaders);
        {_, local} ->
            parse_local_dest(VHostName, ClusterName, Def, SourceHeaders)
    end.

parse_amqp10_dest({_VHost, _Name}, _ClusterName, Def, SourceHeaders) ->
    Uris = deobfuscated_uris(<<"dest-uri">>, Def),
    Address = pget(<<"dest-address">>, Def),
    Properties =
        rabbit_data_coercion:to_proplist(
            pget(<<"dest-properties">>, Def, [])),
    AppProperties =
        rabbit_data_coercion:to_proplist(
            pget(<<"dest-application-properties">>, Def, [])),
    MessageAnns =
        rabbit_data_coercion:to_proplist(
            pget(<<"dest-message-annotations">>, Def, [])),
    #{module => rabbit_amqp10_shovel,
      uris => Uris,
      target_address => Address,
      message_annotations => maps:from_list(MessageAnns),
      application_properties => maps:from_list(AppProperties ++ SourceHeaders),
      properties => maps:from_list(
                      lists:map(fun({K, V}) ->
                                        {rabbit_data_coercion:to_atom(K), V}
                                end, Properties)),
      add_timestamp_header => pget(<<"dest-add-timestamp-header">>, Def, false),
      add_forward_headers => pget(<<"dest-add-forward-headers">>, Def, false),
      unacked => #{}
     }.

parse_amqp091_dest({VHost, Name}, ClusterName, Def, SourceHeaders) ->
    DestURIs  = deobfuscated_uris(<<"dest-uri">>,      Def),
    DestX     = pget(<<"dest-exchange">>,     Def, none),
    DestXKey  = pget(<<"dest-exchange-key">>, Def, none),
    DestQ     = pget(<<"dest-queue">>,        Def, none),
    DestQArgs = pget(<<"dest-queue-args">>,   Def, #{}),
    GlobalPredeclared = proplists:get_value(predeclared, application:get_env(?APP, topology, []), false),
    Predeclared = pget(<<"dest-predeclared">>, Def, GlobalPredeclared),
    DestDeclFun = case Predeclared of
        true -> {?MODULE, dest_check, [DestQ, DestQArgs]};
        false -> {?MODULE, dest_decl, [DestQ, DestQArgs]}
    end,

    {X, Key} = case DestQ of
                   none -> {DestX, DestXKey};
                   _    -> {<<>>,  DestQ}
               end,
    Table2 = [{K, V} || {K, V} <- [{<<"dest-exchange">>,     DestX},
                                   {<<"dest-exchange-key">>, DestXKey},
                                   {<<"dest-queue">>,        DestQ}],
                        V =/= none],
    AddHeadersLegacy = pget(<<"add-forward-headers">>, Def, false),
    AddHeaders = pget(<<"dest-add-forward-headers">>, Def, AddHeadersLegacy),
    Table0 = [{<<"shovelled-by">>, ClusterName},
              {<<"shovel-type">>,  <<"dynamic">>},
              {<<"shovel-name">>,  Name},
              {<<"shovel-vhost">>, VHost}],
    SetProps = lookup_indices(pget(<<"dest-publish-properties">>, Def,
                                   pget(<<"publish-properties">>, Def, [])),
                              record_info(fields, 'P_basic')),
    AddTimestampHeaderLegacy = pget(<<"add-timestamp-header">>, Def, false),
    AddTimestampHeader = pget(<<"dest-add-timestamp-header">>, Def,
                              AddTimestampHeaderLegacy),
    %% Details are only used for status report in rabbitmqctl, as vhost is not
    %% available to query the runtime parameters.
    Details = maps:from_list([{K, V} || {K, V} <- [{dest_exchange, DestX},
                                                   {dest_exchange_key, DestXKey},
                                                   {dest_queue, DestQ}],
                                        V =/= none]),
    maps:merge(#{module => rabbit_amqp091_shovel,
                 uris => DestURIs,
                 resource_decl => DestDeclFun,
                 fields_fun => {?MODULE, fields_fun, [X, Key]},
                 props_fun => {?MODULE, props_fun, [Table0, Table2, SetProps,
                                                    AddHeaders, SourceHeaders,
                                                    AddTimestampHeader]}
                }, Details).

parse_local_dest({_VHost, _Name}, _ClusterName, Def, _SourceHeaders) ->
    Mod       = rabbit_local_shovel,
    DestURIs  = deobfuscated_uris(<<"dest-uri">>,      Def),
    DestX     = pget(<<"dest-exchange">>,     Def, none),
    DestXKey  = pget(<<"dest-exchange-key">>, Def, none),
    DestQ     = pget(<<"dest-queue">>,        Def, none),
    DestQArgs = pget(<<"dest-queue-args">>,   Def, #{}),
    GlobalPredeclared = proplists:get_value(predeclared, application:get_env(?APP, topology, []), false),
    Predeclared = pget(<<"dest-predeclared">>, Def, GlobalPredeclared),
    DestDeclFun = case Predeclared of
        true -> {Mod, dest_check_queue, [DestQ, DestQArgs]};
        false -> {Mod, dest_decl_queue, [DestQ, DestQArgs]}
    end,

    AddHeaders = pget(<<"dest-add-forward-headers">>, Def, false),
    AddTimestampHeader = pget(<<"dest-add-timestamp-header">>, Def, false),
    %% Details are only used for status report in rabbitmqctl, as vhost is not
    %% available to query the runtime parameters.
    Details = maps:from_list([{K, V} || {K, V} <- [{exchange, DestX},
                                                   {routing_key, DestXKey},
                                                   {queue, DestQ}],
                                        V =/= none]),
    maps:merge(#{module => rabbit_local_shovel,
                 uris => DestURIs,
                 resource_decl => DestDeclFun,
                 add_forward_headers => AddHeaders,
                 add_timestamp_header => AddTimestampHeader
                }, Details).

fields_fun(X, Key, _SrcURI, _DestURI, P0) ->
    P1 = case X of
             none -> P0;
             _    -> P0#'basic.publish'{exchange = X}
         end,
    case Key of
        none -> P1;
        _    -> P1#'basic.publish'{routing_key = Key}
    end.

props_fun(Table0, Table2, SetProps, AddHeaders, SourceHeaders, AddTimestampHeader,
          SrcURI, DestURI, P0) ->
    P  = set_properties(P0, SetProps),
    P1 = case AddHeaders of
             true -> rabbit_shovel_util:update_headers(
                       Table0, SourceHeaders ++ Table2,
                       SrcURI, DestURI, P);
             false -> P
         end,
    case AddTimestampHeader of
        true  -> rabbit_shovel_util:add_timestamp_header(P1);
        false -> P1
    end.

dest_decl(DestQ, DestQArgs, Conn, _Ch) ->
    case DestQ of
        none -> ok;
        _ -> ensure_queue(Conn, DestQ, rabbit_misc:to_amqp_table(DestQArgs))
    end.
dest_check(DestQ, DestQArgs, Conn, _Ch) ->
    case DestQ of
        none -> ok;
        _ -> check_queue(Conn, DestQ, rabbit_misc:to_amqp_table(DestQArgs))
    end.

parse_amqp10_source(Def) ->
    Uris = deobfuscated_uris(<<"src-uri">>, Def),
    Address = pget(<<"src-address">>, Def),
    DeleteAfter = pget(<<"src-delete-after">>, Def, <<"never">>),
    PrefetchCount = pget(<<"src-prefetch-count">>, Def, 1000),
    Headers = [],
    {#{module => rabbit_amqp10_shovel,
       uris => Uris,
       source_address => Address,
       delete_after => opt_b2a(DeleteAfter),
       prefetch_count => PrefetchCount,
       consumer_args => []}, Headers}.

parse_amqp091_source(Def) ->
    SrcURIs  = deobfuscated_uris(<<"src-uri">>, Def),
    SrcX     = pget(<<"src-exchange">>,Def, none),
    SrcXKey  = pget(<<"src-exchange-key">>, Def, <<>>), %% [1]
    SrcQ     = pget(<<"src-queue">>, Def, none),
    SrcQArgs = pget(<<"src-queue-args">>,   Def, #{}),
    SrcCArgs = rabbit_misc:to_amqp_table(pget(<<"src-consumer-args">>, Def, [])),
    GlobalPredeclared = proplists:get_value(predeclared, application:get_env(?APP, topology, []), false),
    Predeclared = pget(<<"src-predeclared">>, Def, GlobalPredeclared),
    {SrcDeclFun, Queue, DestHeaders} =
    case SrcQ of
        none -> {{?MODULE, src_decl_exchange, [SrcX, SrcXKey]}, <<>>,
                 [{<<"src-exchange">>,     SrcX},
                  {<<"src-exchange-key">>, SrcXKey}]};
        _ -> case Predeclared of
                false ->
                    {{?MODULE, src_decl_queue, [SrcQ, SrcQArgs]},
                        SrcQ, [{<<"src-queue">>, SrcQ}]};
                true ->
                    {{?MODULE, src_check_queue, [SrcQ, SrcQArgs]},
                        SrcQ, [{<<"src-queue">>, SrcQ}]}
            end
    end,
    DeleteAfter = pget(<<"src-delete-after">>, Def,
                       pget(<<"delete-after">>, Def, <<"never">>)),
    PrefetchCount = pget(<<"src-prefetch-count">>, Def,
                         pget(<<"prefetch-count">>, Def, 1000)),
    %% Details are only used for status report in rabbitmqctl, as vhost is not
    %% available to query the runtime parameters.
    Details = maps:from_list([{K, V} || {K, V} <- [{source_exchange, SrcX},
                                                   {source_exchange_key, SrcXKey}],
                                        V =/= none]),
    {maps:merge(#{module => rabbit_amqp091_shovel,
                  uris => SrcURIs,
                  resource_decl => SrcDeclFun,
                  queue => Queue,
                  delete_after => opt_b2a(DeleteAfter),
                  prefetch_count => PrefetchCount,
                  consumer_args => SrcCArgs
                 }, Details), DestHeaders}.

parse_local_source(Def) ->
    %% TODO add exchange source back
    Mod      = rabbit_local_shovel,
    SrcURIs  = deobfuscated_uris(<<"src-uri">>, Def),
    SrcX     = pget(<<"src-exchange">>,Def, none),
    SrcXKey  = pget(<<"src-exchange-key">>, Def, <<>>),
    SrcQ     = pget(<<"src-queue">>, Def, none),
    SrcQArgs = pget(<<"src-queue-args">>,   Def, #{}),
    SrcCArgs = rabbit_misc:to_amqp_table(pget(<<"src-consumer-args">>, Def, [])),
    GlobalPredeclared = proplists:get_value(predeclared, application:get_env(?APP, topology, []), false),
    Predeclared = pget(<<"src-predeclared">>, Def, GlobalPredeclared),
    {SrcDeclFun, Queue, DestHeaders} =
    case SrcQ of
        none -> {{Mod, src_decl_exchange, [SrcX, SrcXKey]}, <<>>,
                 [{<<"src-exchange">>,     SrcX},
                  {<<"src-exchange-key">>, SrcXKey}]};
        _ -> case Predeclared of
                false ->
                    {{Mod, decl_queue, [SrcQ, SrcQArgs]},
                        SrcQ, [{<<"src-queue">>, SrcQ}]};
                true ->
                    {{Mod, check_queue, [SrcQ, SrcQArgs]},
                        SrcQ, [{<<"src-queue">>, SrcQ}]}
            end
    end,
    DeleteAfter = pget(<<"src-delete-after">>, Def,
                       pget(<<"delete-after">>, Def, <<"never">>)),
    %% Details are only used for status report in rabbitmqctl, as vhost is not
    %% available to query the runtime parameters.
    Details = maps:from_list([{K, V} || {K, V} <- [{exchange, SrcX},
                                                   {routing_key, SrcXKey}],
                                        V =/= none]),
    {maps:merge(#{module => Mod,
                  uris => SrcURIs,
                  resource_decl => SrcDeclFun,
                  queue => Queue,
                  delete_after => opt_b2a(DeleteAfter),
                  consumer_args => SrcCArgs
                 }, Details), DestHeaders}.

src_decl_exchange(SrcX, SrcXKey, _Conn, Ch) ->
    Ms = [#'queue.declare'{exclusive = true},
          #'queue.bind'{routing_key = SrcXKey,
                        exchange    = SrcX}],
    [amqp_channel:call(Ch, M) || M <- Ms].

src_decl_queue(SrcQ, SrcQArgs, Conn, _Ch) ->
    ensure_queue(Conn, SrcQ, rabbit_misc:to_amqp_table(SrcQArgs)).

src_check_queue(SrcQ, SrcQArgs, Conn, _Ch) ->
    check_queue(Conn, SrcQ, rabbit_misc:to_amqp_table(SrcQArgs)).

get_uris(Key, Def) ->
    URIs = case pget(Key, Def) of
               B when is_binary(B) -> [B];
               L when is_list(L)   -> L
           end,
    [binary_to_list(URI) || URI <- URIs].

deobfuscated_uris(Key, Def) ->
    ObfuscatedURIs = pget(Key, Def),
    URIs = [credentials_obfuscation:decrypt(ObfuscatedURI) || ObfuscatedURI <- ObfuscatedURIs],
    [binary_to_list(URI) || URI <- URIs].

translate_ack_mode(<<"on-confirm">>) -> on_confirm;
translate_ack_mode(<<"on-publish">>) -> on_publish;
translate_ack_mode(<<"no-ack">>)     -> no_ack.

ensure_queue(Conn, Queue, XArgs) ->
    {ok, Ch} = amqp_connection:open_channel(Conn),
    try
        amqp_channel:call(Ch, #'queue.declare'{queue   = Queue,
                                               passive = true})
    catch exit:{{shutdown, {server_initiated_close, ?NOT_FOUND, _Text}}, _} ->
            {ok, Ch2} = amqp_connection:open_channel(Conn),
            amqp_channel:call(Ch2, #'queue.declare'{queue     = Queue,
                                                    durable   = true,
                                                    arguments = XArgs}),
            catch amqp_channel:close(Ch2)

    after
        catch amqp_channel:close(Ch)
    end.
check_queue(Conn, Queue, _XArgs) ->
    {ok, Ch} = amqp_connection:open_channel(Conn),
    try
        amqp_channel:call(Ch, #'queue.declare'{queue   = Queue,
                                               passive = true})
    after
        catch amqp_channel:close(Ch)
    end.
opt_b2a(B) when is_binary(B) -> list_to_atom(binary_to_list(B));
opt_b2a(N)                   -> N.

set_properties(Props, []) ->
    Props;
set_properties(Props, [{Ix, V} | Rest]) ->
    set_properties(setelement(Ix, Props, V), Rest).

lookup_indices(KVs0, L) ->
    KVs = rabbit_data_coercion:to_proplist(KVs0),
    [{1 + list_find(list_to_atom(binary_to_list(K)), L), V} || {K, V} <- KVs].

list_find(K, L) -> list_find(K, L, 1).

list_find(K, [K|_], N) -> N;
list_find(K, [],   _N) -> exit({not_found, K});
list_find(K, [_|L], N) -> list_find(K, L, N + 1).

protocols(Def) when is_map(Def) ->
    protocols(rabbit_data_coercion:to_proplist(Def));
protocols(Def) ->
    Src = case lists:keyfind(<<"src-protocol">>, 1, Def) of
              {_, SrcProtocol} ->
                  rabbit_data_coercion:to_atom(SrcProtocol);
              false -> amqp091
          end,
    Dst = case lists:keyfind(<<"dest-protocol">>, 1, Def) of
              {_, DstProtocol} ->
                  rabbit_data_coercion:to_atom(DstProtocol);
              false -> amqp091
          end,
    {Src, Dst}.
