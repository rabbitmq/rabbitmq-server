-module(rabbit_data_coercion_test).

-include_lib("eunit/include/eunit.hrl").

to_existing_atom_can_convert_value_to_atom_test() -> 
    Value = this_is_an_atom,
    ?assertEqual(Value, rabbit_data_coercion:to_existing_atom("this_is_an_atom")),
    ?assertEqual(Value, rabbit_data_coercion:to_existing_atom(<<"this_is_an_atom">>)),
    ?assertEqual(Value, rabbit_data_coercion:to_existing_atom(Value)).

to_existing_atom_result_in_error_if_atom_doesnt_exist_test() -> 
    ?assertError(badarg,rabbit_data_coercion:to_existing_atom("this_atom_doesnt_exist")).
