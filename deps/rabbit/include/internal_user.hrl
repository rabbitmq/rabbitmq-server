-define(is_internal_user(U),
        (?is_internal_user_v2(U))).

-define(is_internal_user_v2(U), is_record(U, internal_user, 6)).
