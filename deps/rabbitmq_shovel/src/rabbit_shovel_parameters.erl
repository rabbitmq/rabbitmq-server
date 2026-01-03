%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_parameters).
-behaviour(rabbit_runtime_parameter).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").
-include_lib("kernel/include/logger.hrl").

-export([validate/5, notify/5, notify_clear/4]).
-export([register/0, unregister/0, parse/3]).
-export([obfuscate_uris_in_definition/1]).
-export([src_protocol/1, dest_protocol/1, protocols/1]).
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
    {Protocol, _} = protocols(Def),
    {ok, Mod} = rabbit_registry:lookup_module(shovel_protocol, Protocol),
    Mod:validate_src(Def).

validate_dest(Def) ->
    {_, Protocol} = protocols(Def),
    {ok, Mod} = rabbit_registry:lookup_module(shovel_protocol, Protocol),
    Mod:validate_dest(Def).

obfuscate_uris_in_definition(Def) ->
  SrcURIs  = get_uris(<<"src-uri">>, Def),
  ObfuscatedSrcURIsDef = pset(<<"src-uri">>, obfuscate_uris(SrcURIs), Def),
  DestURIs  = get_uris(<<"dest-uri">>, Def),
  ObfuscatedDef = pset(<<"dest-uri">>, obfuscate_uris(DestURIs), ObfuscatedSrcURIsDef),
  ObfuscatedDef.

obfuscate_uris(URIs) ->
  [credentials_obfuscation:encrypt(URI) || URI <- URIs].

shovel_validation() ->
    AllProtocols = list_all_protocols(),
    [{<<"internal">>, fun rabbit_parameter_validation:boolean/2, optional},
     {<<"internal_owner">>, fun validate_internal_owner/2, optional},
     {<<"reconnect-delay">>, fun rabbit_parameter_validation:number/2,optional},
     {<<"ack-mode">>, rabbit_parameter_validation:enum(
                        ['no-ack', 'on-publish', 'on-confirm']), optional},
     {<<"src-protocol">>,
      rabbit_parameter_validation:enum(AllProtocols), optional},
     {<<"dest-protocol">>,
      rabbit_parameter_validation:enum(AllProtocols), optional}
    ].

src_validation(Def, User) ->
    {Protocol, _} = protocols(Def),
    {ok, Mod} = rabbit_registry:lookup_module(shovel_protocol, Protocol),
    Mod:validate_src_funs(Def, User).

dest_validation(Def0, User) ->
    {_, Protocol} = protocols(Def0),
    {ok, Mod} = rabbit_registry:lookup_module(shovel_protocol, Protocol),
    Mod:validate_dest_funs(Def0, User).

validate_internal_owner(Name, Term0) ->
    Term = rabbit_data_coercion:to_proplist(Term0),

    rabbit_parameter_validation:proplist(Name, [{<<"name">>, fun rabbit_parameter_validation:binary/2},
                                                {<<"kind">>, rabbit_parameter_validation:enum(
                                                              ['exchange', 'queue'])},
                                                {<<"virtual_host">>, fun rabbit_parameter_validation:binary/2}], Term).

src_protocol(Def) when is_map(Def) ->
    src_protocol(rabbit_data_coercion:to_proplist(Def));
src_protocol(Def) when is_list(Def) ->
    case lists:keyfind(<<"src-protocol">>, 1, Def) of
        {_, SrcProtocol} ->
            rabbit_data_coercion:to_atom(SrcProtocol);
        false -> amqp091
    end.

dest_protocol(Def) when is_map(Def) ->
    dest_protocol(rabbit_data_coercion:to_proplist(Def));
dest_protocol(Def) when is_list(Def) ->
    case lists:keyfind(<<"dest-protocol">>, 1, Def) of
        {_, DstProtocol} ->
            rabbit_data_coercion:to_atom(DstProtocol);
        false -> amqp091
    end.

protocols(Def) when is_map(Def) ->
    protocols(rabbit_data_coercion:to_proplist(Def));
protocols(Def) ->
    Src = src_protocol(Def),
    Dst = dest_protocol(Def),
    {Src, Dst}.

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
    {Protocol, _} = protocols(Def),
    {ok, Mod} = rabbit_registry:lookup_module(shovel_protocol, Protocol),
    Mod:parse_source(Def).

parse_dest(VHostName, ClusterName, Def, SourceHeaders) ->
    {_, Protocol} = protocols(Def),
    {ok, Mod} = rabbit_registry:lookup_module(shovel_protocol, Protocol),
    Mod:parse_dest(VHostName, ClusterName, Def, SourceHeaders).

get_uris(Key, Def) ->
    URIs = case pget(Key, Def) of
               B when is_binary(B) -> [B];
               L when is_list(L)   -> L
           end,
    [binary_to_list(URI) || URI <- URIs].

translate_ack_mode(<<"on-confirm">>) -> on_confirm;
translate_ack_mode(<<"on-publish">>) -> on_publish;
translate_ack_mode(<<"no-ack">>)     -> no_ack.

list_all_protocols() ->
    [P || {P, _} <- rabbit_registry:lookup_all(shovel_protocol)].
