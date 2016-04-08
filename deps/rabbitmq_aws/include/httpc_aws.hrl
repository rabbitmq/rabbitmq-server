%%% ====================================================================
%%% @author Gavin M. Roy <gavinmroy@gmail.com>
%%% @copyright 2016, Gavin M. Roy
%%% @doc httpc_aws client library constants and records
%%% @end
%%% ====================================================================

-define(ALGORITHM, "AWS4-HMAC-SHA256").
-define(MIME_AWS_JSON, "application/x-amz-json-1.0").
-define(SCHEME, https).

-define(DEFAULT_PROFILE, "default").
-define(INSTANCE_AZ, ["placement", "availability-zone"]).
-define(INSTANCE_CONNECT_TIMEOUT, 100).
-define(INSTANCE_CREDENTIALS, ["iam", "security-credentials"]).
-define(INSTANCE_IP, "169.254.169.254").
-define(INSTANCE_METADATA_BASE, ["latest", "meta-data"]).
-define(INSTANCE_SCHEME, http).

-record(state, {access_key, secret_access_key, expiration, security_token}).
