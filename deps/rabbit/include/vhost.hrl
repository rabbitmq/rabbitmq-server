-include("vhost_v2.hrl").
-include("vhost_v3.hrl").

-define(is_vhost(V),
        (?is_vhost_v2(V) orelse ?is_vhost_v3(V))).
