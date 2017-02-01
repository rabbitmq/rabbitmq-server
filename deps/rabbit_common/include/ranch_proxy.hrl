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
