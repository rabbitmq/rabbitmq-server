-define(is_vhost_v2(V), is_record(V, vhost, 4)).

-define(vhost_v2_field_name(Q), element(2, Q)).
-define(vhost_v2_field_limits(Q), element(3, Q)).
-define(vhost_v2_field_metadata(Q), element(4, Q)).
