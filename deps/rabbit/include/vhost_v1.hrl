-define(is_vhost_v1(V), is_record(V, vhost, 3)).

-define(vhost_v1_field_name(V), element(2, V)).
-define(vhost_v1_field_limits(V), element(3, V)).
