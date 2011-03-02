%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_util).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([parse_uri/1, purpose_arg/0, has_purpose_arg/1]).

parse_uri(URI) ->
    case rabbit_federation_uri_parser:parse(
           binary_to_list(URI), [{host, undefined}, {path, "/"},
                                 {port, 5672},      {'query', []}]) of
        {error, _} = E ->
            E;
        Props ->
            case string:tokens(proplists:get_value(path, Props), "/") of
                [VHostEnc, XEnc] ->
                    VHost = httpd_util:decode_hex(VHostEnc),
                    X = httpd_util:decode_hex(XEnc),
                    [{exchange, list_to_binary(X)},
                     {vhost,    list_to_binary(VHost)}] ++ Props;
                _ ->
                    {error, path_must_have_two_components}
            end
    end.

purpose_arg() ->
    {<<"x-purpose">>, longstr, <<"federation">>}.

has_purpose_arg(X) ->
    #exchange{arguments = Args} = rabbit_exchange:lookup_or_die(X),
    rabbit_misc:table_lookup(Args, <<"x-purpose">>) ==
        {longstr, <<"federation">>}.
