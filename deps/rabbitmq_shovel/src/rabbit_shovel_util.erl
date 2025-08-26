%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_util).

-export([update_headers/5,
         add_timestamp_header/1,
         delete_shovel/3,
         restart_shovel/2,
         get_shovel_parameter/1,
         gen_unique_name/2,
         decl_fun/2]).

-export([
    dynamic_shovel_supervisor_mod/0
]).

-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").

-define(APP, rabbitmq_shovel).
-define(ROUTING_HEADER, <<"x-shovelled">>).
-define(TIMESTAMP_HEADER, <<"x-shovelled-timestamp">>).

-spec dynamic_shovel_supervisor_mod() -> module().
dynamic_shovel_supervisor_mod() ->
    rabbit_shovel_dyn_worker_sup_sup.

update_headers(Prefix, Suffix, SrcURI, DestURI,
               Props = #'P_basic'{headers = Headers}) ->
    Table = Prefix ++ [{<<"src-uri">>,  SrcURI},
                       {<<"dest-uri">>, DestURI}] ++ Suffix,
    Headers2 = rabbit_basic:prepend_table_header(
                 ?ROUTING_HEADER, [{K, longstr, V} || {K, V} <- Table],
                 Headers),
    Props#'P_basic'{headers = Headers2}.

add_timestamp_header(Props = #'P_basic'{headers = undefined}) ->
    add_timestamp_header(Props#'P_basic'{headers = []});
add_timestamp_header(Props = #'P_basic'{headers = Headers}) ->
    Headers2 = rabbit_misc:set_table_value(Headers,
                                           ?TIMESTAMP_HEADER,
                                           long,
                                           os:system_time(seconds)),
    Props#'P_basic'{headers = Headers2}.

delete_shovel(VHost, Name, ActingUser) ->
    case rabbit_shovel_status:lookup({VHost, Name}) of
        not_found ->
            %% Follow the user's obvious intent and delete the runtime parameter just in case the Shovel is in
            %% a starting-failing-restarting loop. MK.
            ?LOG_INFO("Will delete runtime parameters of shovel '~ts' in virtual host '~ts'", [Name, VHost]),
            ok = rabbit_runtime_parameters:clear(VHost, <<"shovel">>, Name, ActingUser),
            {error, not_found};
        _Obj ->
            ShovelParameters = rabbit_runtime_parameters:value(VHost, <<"shovel">>, Name),
            case needs_force_delete(ShovelParameters, ActingUser) of
                false ->
                    ?LOG_INFO("Will delete runtime parameters of shovel '~ts' in virtual host '~ts'", [Name, VHost]),
                    ok = rabbit_runtime_parameters:clear(VHost, <<"shovel">>, Name, ActingUser);
                true ->
                    report_that_protected_shovel_cannot_be_deleted(Name, VHost, ShovelParameters)
            end
    end.

-spec report_that_protected_shovel_cannot_be_deleted(binary(), binary(), map() | [tuple()]) -> no_return().
report_that_protected_shovel_cannot_be_deleted(Name, VHost, ShovelParameters) ->
    case rabbit_shovel_parameters:internal_owner(ShovelParameters) of
        undefined ->
            rabbit_misc:protocol_error(
              resource_locked,
              "Cannot delete protected shovel '~ts' in virtual host '~ts'.",
              [Name, VHost]);
        IOwner ->
            rabbit_misc:protocol_error(
              resource_locked,
              "Cannot delete protected shovel '~ts' in virtual host '~ts'. It was "
              "declared as protected, delete it with --force or delete its owner entity instead: ~ts",
              [Name, VHost, rabbit_misc:rs(IOwner)])
    end.

needs_force_delete(Parameters,ActingUser) ->
    case rabbit_shovel_parameters:is_internal(Parameters) of
        false ->
            false;
        true ->
            case ActingUser of
                ?INTERNAL_USER -> false;
                _ -> true
            end
    end.

restart_shovel(VHost, Name) ->
    case rabbit_shovel_status:lookup({VHost, Name}) of
        not_found ->
            {error, not_found};
        _Obj ->
            Mod = dynamic_shovel_supervisor_mod(),
            ?LOG_INFO("Shovel '~ts' in virtual host '~ts' will be restarted", [Name, VHost]),
            ok = Mod:stop_child({VHost, Name}),
            {ok, _} = Mod:start_link(),
            ok
    end.

get_shovel_parameter({VHost, ShovelName}) ->
    rabbit_runtime_parameters:lookup(VHost, <<"shovel">>, ShovelName);
get_shovel_parameter(ShovelName) ->
    rabbit_runtime_parameters:lookup(<<"/">>, <<"shovel">>, ShovelName).

gen_unique_name(Pre0, Post0) ->
    Pre = rabbit_data_coercion:to_binary(Pre0),
    Post = rabbit_data_coercion:to_binary(Post0),
    Id = bin_to_hex(crypto:strong_rand_bytes(8)),
    <<Pre/binary, <<"_">>/binary, Id/binary, <<"_">>/binary, Post/binary>>.

bin_to_hex(Bin) ->
    <<<<if N >= 10 -> N -10 + $a;
           true  -> N + $0 end>>
      || <<N:4>> <= Bin>>.

decl_fun(Mod, {source, Endpoint}) ->
    case parse_declaration({proplists:get_value(declarations, Endpoint, []), []}) of
        [] ->
            case proplists:get_value(predeclared, application:get_env(?APP, topology, []), false) of
                true -> case proplists:get_value(queue, Endpoint) of
                            <<>> -> fail({invalid_parameter_value, declarations, {require_non_empty}});
                            Queue -> {Mod, check_fun, [Queue]}
                        end;
                false -> {Mod, decl_fun, []}
            end;
        Decl -> {Mod, decl_fun, [Decl]}
    end;
decl_fun(Mod, {destination, Endpoint}) ->
    Decl = parse_declaration({proplists:get_value(declarations, Endpoint, []), []}),
    {Mod, decl_fun, [Decl]}.

parse_declaration({[], Acc}) ->
    Acc;
parse_declaration({[{Method, Props} | Rest], Acc}) when is_list(Props) ->
    FieldNames = try rabbit_framing_amqp_0_9_1:method_fieldnames(Method)
                 catch exit:Reason -> fail(Reason)
                 end,
    case proplists:get_keys(Props) -- FieldNames of
        []            -> ok;
        UnknownFields -> fail({unknown_fields, Method, UnknownFields})
    end,
    {Res, _Idx} = lists:foldl(
                    fun (K, {R, Idx}) ->
                            NewR = case proplists:get_value(K, Props) of
                                       undefined -> R;
                                       V         -> setelement(Idx, R, V)
                                   end,
                            {NewR, Idx + 1}
                    end, {rabbit_framing_amqp_0_9_1:method_record(Method), 2},
                    FieldNames),
    parse_declaration({Rest, [Res | Acc]});
parse_declaration({[{Method, Props} | _Rest], _Acc}) ->
    fail({expected_method_field_list, Method, Props});
parse_declaration({[Method | Rest], Acc}) ->
    parse_declaration({[{Method, []} | Rest], Acc}).

-spec fail(term()) -> no_return().
fail(Reason) -> throw({error, Reason}).
