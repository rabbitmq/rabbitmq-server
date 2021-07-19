%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_shovel_parameters).
-behaviour(rabbit_runtime_parameter).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").

-export([validate/5, notify/5, notify_clear/4]).
-export([register/0, unregister/0, parse/3]).

-import(rabbit_misc, [pget/2, pget/3]).

-rabbit_boot_step({?MODULE,
                   [{description, "shovel parameters"},
                    {mfa, {rabbit_shovel_parameters, register, []}},
                    {cleanup, {?MODULE, unregister, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    rabbit_registry:register(runtime_parameter, <<"shovel">>, ?MODULE).

unregister() ->
    rabbit_registry:unregister(runtime_parameter, <<"shovel">>).

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
    {error, "name not recognised: ~p", [Name]}.

pget2(K1, K2, Defs) -> case {pget(K1, Defs), pget(K2, Defs)} of
                           {undefined, undefined} -> zero;
                           {undefined, _}         -> one;
                           {_,         undefined} -> one;
                           {_,         _}         -> both
                       end.

notify(VHost, <<"shovel">>, Name, Definition, _Username) ->
    rabbit_shovel_dyn_worker_sup_sup:adjust({VHost, Name}, Definition).

notify_clear(VHost, <<"shovel">>, Name, _Username) ->
    rabbit_shovel_dyn_worker_sup_sup:stop_child({VHost, Name}).

%%----------------------------------------------------------------------------

validate_src(Def) ->
    case protocols(Def)  of
        {amqp091, _} -> validate_amqp091_src(Def);
        {amqp10, _} -> []
    end.

validate_dest(Def) ->
    case protocols(Def)  of
        {_, amqp091} -> validate_amqp091_dest(Def);
        {_, amqp10} -> []
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

validate_amqp091_dest(Def) ->
    [case pget2(<<"dest-exchange">>, <<"dest-queue">>, Def) of
         zero -> ok;
         one  -> ok;
         both -> {error, "Cannot specify 'dest-exchange' and 'dest-queue'", []}
     end].

shovel_validation() ->
    [{<<"reconnect-delay">>, fun rabbit_parameter_validation:number/2,optional},
     {<<"ack-mode">>, rabbit_parameter_validation:enum(
                        ['no-ack', 'on-publish', 'on-confirm']), optional},
     {<<"src-protocol">>,
      rabbit_parameter_validation:enum(['amqp10', 'amqp091']), optional},
     {<<"dest-protocol">>,
      rabbit_parameter_validation:enum(['amqp10', 'amqp091']), optional}
    ].

src_validation(Def, User) ->
    case protocols(Def)  of
        {amqp091, _} -> amqp091_src_validation(Def, User);
        {amqp10, _} -> amqp10_src_validation(Def, User)
    end.


amqp10_src_validation(_Def, User) ->
    [
     {<<"src-uri">>, validate_uri_fun(User), mandatory},
     {<<"src-address">>, fun rabbit_parameter_validation:binary/2, mandatory},
     {<<"src-prefetch-count">>, fun rabbit_parameter_validation:number/2, optional},
     {<<"src-delete-after">>, fun validate_delete_after/2, optional}
    ].

amqp091_src_validation(_Def, User) ->
    [
     {<<"src-uri">>,          validate_uri_fun(User), mandatory},
     {<<"src-exchange">>,     fun rabbit_parameter_validation:binary/2, optional},
     {<<"src-exchange-key">>, fun rabbit_parameter_validation:binary/2, optional},
     {<<"src-queue">>,        fun rabbit_parameter_validation:binary/2, optional},
     {<<"src-queue-args">>,   fun validate_queue_args/2, optional},
     {<<"prefetch-count">>,   fun rabbit_parameter_validation:number/2, optional},
     {<<"src-prefetch-count">>, fun rabbit_parameter_validation:number/2, optional},
     %% a deprecated pre-3.7 setting
     {<<"delete-after">>, fun validate_delete_after/2, optional},
     %% currently used multi-protocol friend name, introduced in 3.7
     {<<"src-delete-after">>, fun validate_delete_after/2, optional}
    ].

dest_validation(Def0, User) ->
    Def = rabbit_data_coercion:to_proplist(Def0),
    case protocols(Def)  of
        {_, amqp091} -> amqp091_dest_validation(Def, User);
        {_, amqp10} -> amqp10_dest_validation(Def, User)
    end.

amqp10_dest_validation(_Def, User) ->
    [{<<"dest-uri">>, validate_uri_fun(User), mandatory},
     {<<"dest-address">>, fun rabbit_parameter_validation:binary/2, mandatory},
     {<<"dest-add-forward-headers">>, fun rabbit_parameter_validation:boolean/2, optional},
     {<<"dest-add-timestamp-header">>, fun rabbit_parameter_validation:boolean/2, optional},
     {<<"dest-application-properties">>, fun validate_amqp10_map/2, optional},
     {<<"dest-message-annotations">>, fun validate_amqp10_map/2, optional},
     % TODO: restrict to allowed fields
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
     {<<"dest-publish-properties">>, fun validate_properties/2,  optional}
    ].

validate_uri_fun(User) ->
    fun (Name, Term) -> validate_uri(Name, Term, User) end.

validate_uri(Name, Term, User) when is_binary(Term) ->
    case rabbit_parameter_validation:binary(Name, Term) of
        ok -> case amqp_uri:parse(binary_to_list(Term)) of
                  {ok, P}    -> validate_params_user(P, User);
                  {error, E} -> {error, "\"~s\" not a valid URI: ~p", [Term, E]}
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
                          _ = rabbit_log:debug("rabbit_access_control:check_vhost_access result: ~p", [NotOK]),
                          NotOK
                  end,
    case rabbit_vhost:exists(VHost) andalso VHostAccess of
        ok -> ok;
        _ ->
            {error, "user \"~s\" may not connect to vhost \"~s\"", [Username, VHost]}
    end;
validate_params_user(#amqp_params_network{}, _User) ->
    ok.

validate_delete_after(_Name, <<"never">>)          -> ok;
validate_delete_after(_Name, <<"queue-length">>)   -> ok;
validate_delete_after(_Name, N) when is_integer(N) -> ok;
validate_delete_after(Name,  Term) ->
    {error, "~s should be number, \"never\" or \"queue-length\", actually was "
     "~p", [Name, Term]}.

validate_queue_args(Name, Term0) ->
    Term = rabbit_data_coercion:to_proplist(Term0),

    rabbit_parameter_validation:proplist(Name, rabbit_amqqueue:declare_args(), Term).

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
        {amqp091, _} -> parse_amqp091_source(Def)
    end.

parse_dest(VHostName, ClusterName, Def, SourceHeaders) ->
    case protocols(Def) of
        {_, amqp10} ->
            parse_amqp10_dest(VHostName, ClusterName, Def, SourceHeaders);
        {_, amqp091} ->
            parse_amqp091_dest(VHostName, ClusterName, Def, SourceHeaders)
    end.

parse_amqp10_dest({_VHost, _Name}, _ClusterName, Def, SourceHeaders) ->
    Uris = get_uris(<<"dest-uri">>, Def),
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
    DestURIs  = get_uris(<<"dest-uri">>,      Def),
    DestX     = pget(<<"dest-exchange">>,     Def, none),
    DestXKey  = pget(<<"dest-exchange-key">>, Def, none),
    DestQ     = pget(<<"dest-queue">>,        Def, none),
    DestQArgs = pget(<<"dest-queue-args">>,   Def, #{}),
    DestDeclFun = fun (Conn, _Ch) ->
                      case DestQ of
                          none -> ok;
                          _ -> ensure_queue(Conn, DestQ, rabbit_misc:to_amqp_table(DestQArgs))
                      end
              end,
    {X, Key} = case DestQ of
                   none -> {DestX, DestXKey};
                   _    -> {<<>>,  DestQ}
               end,
    Table2 = [{K, V} || {K, V} <- [{<<"dest-exchange">>,     DestX},
                                   {<<"dest-exchange-key">>, DestXKey},
                                   {<<"dest-queue">>,        DestQ}],
                        V =/= none],
    PubFun = fun (_SrcURI, _DestURI, P0) ->
                     P1 = case X of
                              none -> P0;
                              _    -> P0#'basic.publish'{exchange = X}
                          end,
                     case Key of
                         none -> P1;
                         _    -> P1#'basic.publish'{routing_key = Key}
                     end
             end,
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
    PubPropsFun = fun (SrcURI, DestURI, P0) ->
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
                      end
                  end,
    %% Details are only used for status report in rabbitmqctl, as vhost is not
    %% available to query the runtime parameters.
    Details = maps:from_list([{K, V} || {K, V} <- [{dest_exchange, DestX},
                                                   {dest_exchange_key, DestXKey},
                                                   {dest_queue, DestQ}],
                                        V =/= none]),
    maps:merge(#{module => rabbit_amqp091_shovel,
                 uris => DestURIs,
                 resource_decl => DestDeclFun,
                 fields_fun => PubFun,
                 props_fun => PubPropsFun
                }, Details).

parse_amqp10_source(Def) ->
    Uris = get_uris(<<"src-uri">>, Def),
    Address = pget(<<"src-address">>, Def),
    DeleteAfter = pget(<<"src-delete-after">>, Def, <<"never">>),
    PrefetchCount = pget(<<"src-prefetch-count">>, Def, 1000),
    Headers = [],
    {#{module => rabbit_amqp10_shovel,
       uris => Uris,
       source_address => Address,
       delete_after => opt_b2a(DeleteAfter),
       prefetch_count => PrefetchCount}, Headers}.

parse_amqp091_source(Def) ->
    SrcURIs  = get_uris(<<"src-uri">>, Def),
    SrcX     = pget(<<"src-exchange">>,Def, none),
    SrcXKey  = pget(<<"src-exchange-key">>, Def, <<>>), %% [1]
    SrcQ     = pget(<<"src-queue">>, Def, none),
    SrcQArgs = pget(<<"src-queue-args">>,   Def, #{}),
    {SrcDeclFun, Queue, DestHeaders} =
    case SrcQ of
        none -> {fun (_Conn, Ch) ->
                         Ms = [#'queue.declare'{exclusive = true},
                               #'queue.bind'{routing_key = SrcXKey,
                                             exchange    = SrcX}],
                         [amqp_channel:call(Ch, M) || M <- Ms]
                 end, <<>>, [{<<"src-exchange">>,     SrcX},
                             {<<"src-exchange-key">>, SrcXKey}]};
        _ -> {fun (Conn, _Ch) ->
                      ensure_queue(Conn, SrcQ, rabbit_misc:to_amqp_table(SrcQArgs))
              end, SrcQ, [{<<"src-queue">>, SrcQ}]}
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
                  prefetch_count => PrefetchCount
                 }, Details), DestHeaders}.

get_uris(Key, Def) ->
    URIs = case pget(Key, Def) of
               B when is_binary(B) -> [B];
               L when is_list(L)   -> L
           end,
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
