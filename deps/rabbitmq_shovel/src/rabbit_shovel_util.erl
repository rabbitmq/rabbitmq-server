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
         get_shovel_parameter/1]).

-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(ROUTING_HEADER, <<"x-shovelled">>).
-define(TIMESTAMP_HEADER, <<"x-shovelled-timestamp">>).

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
            rabbit_log:info("Will delete runtime parameters of shovel '~ts' in virtual host '~ts'", [Name, VHost]),
            ok = rabbit_runtime_parameters:clear(VHost, <<"shovel">>, Name, ActingUser),
            {error, not_found};
        _Obj ->
            ShovelParameters = rabbit_runtime_parameters:value(VHost, <<"shovel">>, Name),
            case needs_force_delete(ShovelParameters, ActingUser) of
                false ->
                    rabbit_log:info("Will delete runtime parameters of shovel '~ts' in virtual host '~ts'", [Name, VHost]),
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
            rabbit_log_shovel:info("Shovel '~ts' in virtual host '~ts' will be restarted", [Name, VHost]),
            ok = rabbit_shovel_dyn_worker_sup_sup:stop_child({VHost, Name}),
            {ok, _} = rabbit_shovel_dyn_worker_sup_sup:start_link(),
            ok
    end.

get_shovel_parameter({VHost, ShovelName}) ->
    rabbit_runtime_parameters:lookup(VHost, <<"shovel">>, ShovelName);
get_shovel_parameter(ShovelName) ->
    rabbit_runtime_parameters:lookup(<<"/">>, <<"shovel">>, ShovelName).
