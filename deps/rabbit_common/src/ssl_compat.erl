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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(ssl_compat).

%% We don't want warnings about the use of ssl:connection_info/1 in this
%% module.
-compile(nowarn_deprecated_function).

%% Declare versioned functions to allow dynamic code loading,
%% depending on the Erlang version running. See 'code_version.erl' for details
-erlang_version_support(
   [{18, [{connection_information, 1, connection_information_pre_18,
           connection_information_post_18},
          {connection_information, 2, connection_information_pre_18,
           connection_information_post_18}]}
   ]).

-export([connection_information/1,
         connection_information_pre_18/1,
         connection_information_post_18/1,
         connection_information/2,
         connection_information_pre_18/2,
         connection_information_post_18/2]).

connection_information(SslSocket) ->
    code_version:update(?MODULE),
    ssl_compat:connection_information(SslSocket).

connection_information_post_18(SslSocket) ->
    ssl:connection_information(SslSocket).

connection_information_pre_18(SslSocket) ->
    case ssl:connection_info(SslSocket) of
        {ok, {ProtocolVersion, CipherSuite}} ->
            {ok, [{protocol, ProtocolVersion},
                  {cipher_suite, CipherSuite}]};
        {error, Reason} ->
            {error, Reason}
    end.

connection_information(SslSocket, Items) ->
    code_version:update(?MODULE),
    ssl_compat:connection_information(SslSocket, Items).

connection_information_post_18(SslSocket, Items) ->
    ssl:connection_information(SslSocket, Items).

connection_information_pre_18(SslSocket, Items) ->
    WantProtocolVersion = lists:member(protocol, Items),
    WantCipherSuite = lists:member(cipher_suite, Items),
    if
        WantProtocolVersion orelse WantCipherSuite ->
            case ssl:connection_info(SslSocket) of
                {ok, {ProtocolVersion, CipherSuite}} ->
                    filter_information_items(ProtocolVersion,
                                             CipherSuite,
                                             Items,
                                             []);
                {error, Reason} ->
                    {error, Reason}
            end;
        true ->
            {ok, []}
    end.

filter_information_items(ProtocolVersion, CipherSuite, [protocol | Rest],
  Result) ->
    filter_information_items(ProtocolVersion, CipherSuite, Rest,
      [{protocol, ProtocolVersion} | Result]);
filter_information_items(ProtocolVersion, CipherSuite, [cipher_suite | Rest],
  Result) ->
    filter_information_items(ProtocolVersion, CipherSuite, Rest,
      [{cipher_suite, CipherSuite} | Result]);
filter_information_items(ProtocolVersion, CipherSuite, [_ | Rest],
  Result) ->
    filter_information_items(ProtocolVersion, CipherSuite, Rest, Result);
filter_information_items(_ProtocolVersion, _CipherSuite, [], Result) ->
    {ok, lists:reverse(Result)}.
