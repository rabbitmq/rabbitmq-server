%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2015 Pivotal Software, Inc.  All rights reserved.
%%

%% Alias for cowboy_static that accepts a list of directories
%% where static files can be found.

-module(rabbit_mgmt_wm_static).

-export([init/3]).
-export([rest_init/2]).
-export([malformed_request/2]).
-export([forbidden/2]).
-export([content_types_provided/2]).
-export([resource_exists/2]).
-export([last_modified/2]).
-export([generate_etag/2]).
-export([get_file/2]).

init(Transport, Req, Opts) ->
    cowboy_static:init(Transport, Req, Opts).

rest_init(Req, [Path]) ->
    cowboy_static:rest_init(Req, {dir, Path});
rest_init(Req, [Path|Tail]) ->
    {PathInfo, _} = cowboy_req:path_info(Req),
    Filepath = filename:join([Path|PathInfo]),
    case filelib:is_regular(Filepath) of
        true -> cowboy_static:rest_init(Req, {dir, Path});
        false -> rest_init(Req, Tail)
    end.

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
