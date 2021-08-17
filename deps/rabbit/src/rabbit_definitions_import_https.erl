%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_definitions_import_https).
-include_lib("rabbit_common/include/rabbit.hrl").

-export([
    is_enabled/0,
    load/1
]).



-import(rabbit_misc, [pget/2, pget/3]).
-import(rabbit_data_coercion, [to_binary/1]).
-import(rabbit_definitions, [import_raw/1]).

%%
%% API
%%

-spec is_enabled() -> boolean().
is_enabled() ->
    case application:get_env(rabbit, definitions) of
        undefined   -> false;
        {ok, none}  -> false;
        {ok, []}    -> false;
        {ok, Proplist} ->
            case proplists:get_value(import_backend, Proplist, undefined) of
                undefined -> false;
                ?MODULE   -> true;
                _         -> false
            end
    end.

load(Proplist) ->
    rabbit_log:debug("Definitions proprties: ~p", [Proplist]),
    URL = pget(url, Proplist),
    TLSOptions0 = [
        %% avoids a peer verification warning emitted by default if no certificate chain and peer verification
        %% settings are provided: these are not essential in this particular case (client-side downloads that likely
        %% will happen from a local trusted source)
        {log_level, error},
        %% use TLSv1.2 by default
        {versions, ['tlsv1.2']}
    ],
    TLSOptions = pget(ssl_options, Proplist, TLSOptions0),
    HTTPOptions = [
        {ssl, TLSOptions}
    ],
    load_from_url(URL, HTTPOptions).


%%
%% Implementation
%%

load_from_url(URL, HTTPOptions0) ->
    inets:start(),
    Options = [
        {body_format, binary}
    ],
    HTTPOptions = HTTPOptions0 ++ [
        {connect_timeout, 120000},
        {autoredirect, true}
    ],
    rabbit_log:info("Applying definitions from remote URL"),
    case httpc:request(get, {URL, []}, lists:usort(HTTPOptions), Options) of
        %% 2XX
        {ok, {{_, Code, _}, _Headers, Body}} when Code div 100 == 2 ->
            rabbit_log:debug("Requested definitions from remote URL '~s', response code: ~b", [URL, Code]),
            rabbit_log:debug("Requested definitions from remote URL '~s', body: ~p", [URL, Body]),
            import_raw(Body);
        {ok, {{_, Code, _}, _Headers, _Body}} when Code >= 400 ->
            rabbit_log:debug("Requested definitions from remote URL '~s', response code: ~b", [URL, Code]),
            {error, {could_not_read_defs, {URL, rabbit_misc:format("URL request failed with response code ~b", [Code])}}};
        {error, Reason} ->
            rabbit_log:error("Requested definitions from remote URL '~s', error: ~p", [URL, Reason]),
            {error, {could_not_read_defs, {URL, Reason}}}
    end.
