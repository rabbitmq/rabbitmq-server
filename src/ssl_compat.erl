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
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(ssl_compat).

%% We don't want warnings about the use of erlang:now/0 in
%% this module.
-compile(nowarn_deprecated_function).

-export([connection_information/1,
         connection_information/2]).

connection_information(SslSocket) ->
    try
        ssl:connection_information(SslSocket)
    catch
        error:undef ->
            case ssl:connection_info(SslSocket) of
                {ok, {ProtocolVersion, CipherSuite}} ->
                    {ok, [{protocol, ProtocolVersion},
                          {cipher_suite, CipherSuite}]};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

connection_information(SslSocket, Items) ->
    try
        ssl:connection_information(SslSocket, Items)
    catch
        error:undef ->
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
            end
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
