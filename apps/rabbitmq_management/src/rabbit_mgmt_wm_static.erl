%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2010-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% Alias for cowboy_static that accepts a list of directories
%% where static files can be found.

-module(rabbit_mgmt_wm_static).

-include_lib("kernel/include/file.hrl").

-export([init/2]).
-export([malformed_request/2]).
-export([forbidden/2]).
-export([content_types_provided/2]).
-export([resource_exists/2]).
-export([last_modified/2]).
-export([generate_etag/2]).
-export([get_file/2]).


init(Req0, {priv_file, _App, _Path}=Opts) ->
    Req1 = rabbit_mgmt_headers:set_common_permission_headers(Req0, ?MODULE),
    cowboy_static:init(Req1, Opts);
init(Req0, [{App, Path}]) ->
    Req1 = rabbit_mgmt_headers:set_common_permission_headers(Req0, ?MODULE),
    do_init(Req1, App, Path);
init(Req0, [{App, Path}|Tail]) ->
    Req1 = rabbit_mgmt_headers:set_common_permission_headers(Req0, ?MODULE),
    PathInfo = cowboy_req:path_info(Req1),
    Filepath = filename:join([code:priv_dir(App), Path|PathInfo]),
    %% We use erl_prim_loader because the file may be inside an .ez archive.
    FileInfo = erl_prim_loader:read_file_info(binary_to_list(Filepath)),
    case FileInfo of
        {ok, #file_info{type = regular}} -> do_init(Req1, App, Path);
        {ok, #file_info{type = symlink}} -> do_init(Req1, App, Path);
        _                                -> init(Req0, Tail)
    end.

do_init(Req, App, Path) ->
    cowboy_static:init(Req, {priv_dir, App, Path}).

malformed_request(Req, State) ->
    cowboy_static:malformed_request(Req, State).

forbidden(Req, State) ->
    cowboy_static:forbidden(Req, State).

content_types_provided(Req, State) ->
    cowboy_static:content_types_provided(Req, State).

resource_exists(Req, State) ->
    cowboy_static:resource_exists(Req, State).

last_modified(Req, State) ->
    cowboy_static:last_modified(Req, State).

generate_etag(Req, State) ->
    cowboy_static:generate_etag(Req, State).

get_file(Req, State) ->
    cowboy_static:get_file(Req, State).
