-define(PT_INVENTORY_KEY, rabbit_ff_registry).
%% The `persistent_term' key used to hold the feature flag inventory.
%%
%% `persistent_term:get(?PT_INVENTORY_KEY)' should return a value with the type
%% `rabbit_feature_flags:inventory()' if the registry is initialized.
%%
%% Rather than fetching this key directly, use the functions in the
%% `rabbit_ff_registry' module.
