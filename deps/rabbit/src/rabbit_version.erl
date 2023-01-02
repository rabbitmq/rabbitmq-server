%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_version).

-export([check_version_consistency/3,
         check_version_consistency/4, check_otp_consistency/1,
         version_error/3]).

%% --------------------------------------------------------------------

-spec check_version_consistency
        (string(), string(), string()) -> rabbit_types:ok_or_error(any()).

check_version_consistency(This, Remote, Name) ->
    check_version_consistency(This, Remote, Name, fun (A, B) -> A =:= B end).

-spec check_version_consistency
        (string(), string(), string(),
         fun((string(), string()) -> boolean())) ->
    rabbit_types:ok_or_error(any()).

check_version_consistency(This, Remote, Name, Comp) ->
    case Comp(This, Remote) of
        true  -> ok;
        false -> version_error(Name, This, Remote)
    end.

version_error(Name, This, Remote) ->
    {error, {inconsistent_cluster,
             rabbit_misc:format("~ts version mismatch: local node is ~ts, "
                                "remote node ~ts", [Name, This, Remote])}}.

-spec check_otp_consistency
        (string()) -> rabbit_types:ok_or_error(any()).

check_otp_consistency(Remote) ->
    check_version_consistency(rabbit_misc:otp_release(), Remote, "OTP").
