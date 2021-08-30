-include("vhost_v1.hrl").
-include("vhost_v2.hrl").

-define(is_vhost(V),
        (?is_vhost_v2(V) orelse
         ?is_vhost_v1(V))).
