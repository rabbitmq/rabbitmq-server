-define(
   IS_FEATURE_FLAG(FeatureProps),
   (is_map(FeatureProps) andalso not ?IS_DEPRECATION(FeatureProps))).

-define(
   IS_DEPRECATION(FeatureProps),
   (is_map(FeatureProps) andalso is_map_key(deprecation_phase, FeatureProps))).
