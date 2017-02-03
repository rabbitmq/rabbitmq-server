%% Copyright (c) 2012, Heroku Inc.
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted provided that the following conditions are
%% met:
%%
%% * Redistributions of source code must retain the above copyright
%%   notice, this list of conditions and the following disclaimer.
%%
%% * Redistributions in binary form must reproduce the above copyright
%%   notice, this list of conditions and the following disclaimer in the
%%   documentation and/or other materials provided with the distribution.
%%
%% * The names of its contributors may not be used to endorse or promote
%%   products derived from this software without specific prior written
%%   permission.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
%% "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
%% LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
%% A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
%% OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
%% SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
%% LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
%% DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
%% THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
%% (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
%% OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

%%% proxy2 defines
-define(HEADER, "\r\n\r\n\0\r\nQUIT\n").
-define(VSN, 16#02).

%% Protocol types
-define(AF_UNSPEC, 16#00).
-define(AF_INET, 16#01).
-define(AF_INET6, 16#02).
-define(AF_UNIX, 16#03).

%% Transfer types
-define(UNSPEC, 16#00).
-define(STREAM, 16#01).
-define(DGRAM, 16#02).

%% TLV types for additional headers
-define(PP2_TYPE_ALPN, 16#01).
-define(PP2_TYPE_AUTHORITY, 16#02).
-define(PP2_TYPE_SSL, 16#20).
-define(PP2_SUBTYPE_SSL_VERSION, 16#21).
-define(PP2_SUBTYPE_SSL_CN, 16#22).
-define(PP2_TYPE_NETNS, 16#30).

%% SSL Client fields
-define(PP2_CLIENT_SSL, 16#01).
-define(PP2_CLIENT_CERT_CONN, 16#02).
-define(PP2_CLIENT_CERT_SESS, 16#04).
