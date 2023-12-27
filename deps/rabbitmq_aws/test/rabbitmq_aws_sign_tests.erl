-module(rabbitmq_aws_sign_tests).

-include_lib("eunit/include/eunit.hrl").
-include("rabbitmq_aws.hrl").


amz_date_test_() ->
  [
    {"value", fun() ->
      ?assertEqual("20160220",
                   rabbitmq_aws_sign:amz_date("20160220T120000Z"))
     end}
  ].


append_headers_test_() ->
  [
    {"with security token", fun() ->

      Headers = [{"Content-Type", "application/x-amz-json-1.0"},
                 {"X-Amz-Target", "DynamoDB_20120810.DescribeTable"}],

      AMZDate = "20160220T120000Z",
      ContentLength = 128,
      PayloadHash = "c888ac0919d062cee1d7b97f44f2a765e4dc9270bc720ba32b8d9f8720626213",
      Hostname = "ec2.amazonaws.com",
      SecurityToken = "AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/L",
      Expectation = [{"content-length", integer_to_list(ContentLength)},
                     {"content-type", "application/x-amz-json-1.0"},
                     {"date", AMZDate},
                     {"host", Hostname},
                     {"x-amz-content-sha256", PayloadHash},
                     {"x-amz-security-token", SecurityToken},
                     {"x-amz-target", "DynamoDB_20120810.DescribeTable"}],
      ?assertEqual(Expectation,
                   rabbitmq_aws_sign:append_headers(AMZDate, ContentLength,
                                                 PayloadHash, Hostname,
                                                 SecurityToken, Headers))
     end},
    {"without security token", fun() ->

      Headers = [{"Content-Type", "application/x-amz-json-1.0"},
                 {"X-Amz-Target", "DynamoDB_20120810.DescribeTable"}],

      AMZDate = "20160220T120000Z",
      ContentLength = 128,
      PayloadHash = "c888ac0919d062cee1d7b97f44f2a765e4dc9270bc720ba32b8d9f8720626213",
      Hostname = "ec2.amazonaws.com",
      Expectation = [{"content-length", integer_to_list(ContentLength)},
                     {"content-type", "application/x-amz-json-1.0"},
                     {"date", AMZDate},
                     {"host", Hostname},
                     {"x-amz-content-sha256", PayloadHash},
                     {"x-amz-target", "DynamoDB_20120810.DescribeTable"}],
      ?assertEqual(Expectation,
                   rabbitmq_aws_sign:append_headers(AMZDate, ContentLength,
                                                 PayloadHash, Hostname,
                                                 undefined, Headers))
     end}
  ].


authorization_header_test_() ->
  [
    {"value", fun() ->
      AccessKey = "AKIDEXAMPLE",
      SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
      RequestTimestamp = "20150830T123600Z",
      Region = "us-east-1",
      Service = "iam",
      Headers = [{"Content-Type", "application/x-www-form-urlencoded; charset=utf-8"},
                 {"Host", "iam.amazonaws.com"},
                 {"Date", "20150830T123600Z"}],
      RequestHash = "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59",
      Expectation = "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;date;host, Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7",
      ?assertEqual(Expectation,
                   rabbitmq_aws_sign:authorization(AccessKey, SecretKey, RequestTimestamp,
                                                Region, Service, Headers, RequestHash))
     end}
  ].


canonical_headers_test_() ->
  [
    {"with security token", fun() ->
      Value = [{"Host", "iam.amazonaws.com"},
               {"Content-Type", "content-type:application/x-www-form-urlencoded; charset=utf-8"},
               {"My-Header2", "\"a b c \""},
               {"My-Header1", "a b c"},
               {"Date", "20150830T123600Z"}],
      Expectation = lists:flatten([
        "content-type:content-type:application/x-www-form-urlencoded; charset=utf-8\n",
        "date:20150830T123600Z\n",
        "host:iam.amazonaws.com\n",
        "my-header1:a b c\n",
        "my-header2:\"a b c \"\n"]),
      ?assertEqual(Expectation, rabbitmq_aws_sign:canonical_headers(Value))
     end}
  ].

credential_scope_test_() ->
  [
    {"string value", fun() ->
      RequestDate = "20150830",
      Region = "us-east-1",
      Service = "iam",
      Expectation = "20150830/us-east-1/iam/aws4_request",
      ?assertEqual(Expectation,
                   rabbitmq_aws_sign:credential_scope(RequestDate, Region, Service))
     end}
  ].

hmac_sign_test_() ->
  [
    {"signed value", fun() ->
      ?assertEqual([84, 114, 243, 48, 184, 73, 81, 138, 195, 123, 62, 27, 222, 141, 188, 149, 178, 82, 252, 75, 29, 34, 102, 186, 98, 232, 224, 105, 64, 6, 119, 33],
                   rabbitmq_aws_sign:hmac_sign("sixpence", "burn the witch"))
     end}
  ].

query_string_test_() ->
  [
    {"properly sorted", fun() ->
      QArgs = [{"Version", "2015-10-01"},
               {"Action", "RunInstances"},
               {"x-amz-algorithm", "AWS4-HMAC-SHA256"},
               {"Date", "20160220T120000Z"},
               {"x-amz-credential", "AKIDEXAMPLE/20140707/us-east-1/ec2/aws4_request"}],
      Expectation = "Action=RunInstances&Date=20160220T120000Z&Version=2015-10-01&x-amz-algorithm=AWS4-HMAC-SHA256&x-amz-credential=AKIDEXAMPLE%2F20140707%2Fus-east-1%2Fec2%2Faws4_request",
      ?assertEqual(Expectation,
                   rabbitmq_aws_sign:query_string(QArgs))
     end},
    {"undefined", fun() ->
      ?assertEqual([], rabbitmq_aws_sign:query_string(undefined))
     end}
  ].

request_hash_test_() ->
  [
    {"hash value", fun() ->
      Method = get,
      Path = "/",
      QArgs = [{"Action", "ListUsers"}, {"Version", "2010-05-08"}],
      Headers = [{"Content-Type", "application/x-www-form-urlencoded; charset=utf-8"},
                 {"Host", "iam.amazonaws.com"},
                 {"Date", "20150830T123600Z"}],
      Payload = "",
      Expectation = "49b454e0f20fe17f437eaa570846fc5d687efc1752c8b5a1eeee5597a7eb92a5",
      ?assertEqual(Expectation,
                   rabbitmq_aws_sign:request_hash(Method, Path, QArgs, Headers, Payload))
     end}
  ].

signature_test_() ->
  [
    {"value", fun() ->
      StringToSign = "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\nf536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59",
      SigningKey = [196, 175, 177, 204, 87, 113, 216, 113, 118, 58, 57, 62, 68, 183, 3, 87, 27, 85, 204, 40, 66, 77, 26, 94, 134, 218, 110, 211, 193, 84, 164, 185],
      Expectation = "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7",
      ?assertEqual(Expectation, rabbitmq_aws_sign:signature(StringToSign, SigningKey))
     end}
  ].


signed_headers_test_() ->
  [
    {"with security token", fun() ->
      Value = [{"X-Amz-Security-Token", "AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/L"},
               {"Date", "20160220T120000Z"},
               {"Content-Type", "application/x-amz-json-1.0"},
               {"Host", "ec2.amazonaws.com"},
               {"Content-Length", 128},
               {"X-Amz-Content-sha256", "c888ac0919d062cee1d7b97f44f2a765e4dc9270bc720ba32b8d9f8720626213"},
               {"X-Amz-Target", "DynamoDB_20120810.DescribeTable"}],
      Expectation = "content-length;content-type;date;host;x-amz-content-sha256;x-amz-security-token;x-amz-target",
      ?assertEqual(Expectation, rabbitmq_aws_sign:signed_headers(Value))
     end}
  ].

signing_key_test_() ->
  [
    {"signing key value", fun() ->
      SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
      AMZDate = "20150830",
      Region = "us-east-1",
      Service = "iam",
      Expectation = [196, 175, 177, 204, 87, 113, 216, 113, 118, 58, 57, 62, 68, 183, 3, 87, 27, 85, 204, 40, 66, 77, 26, 94, 134, 218, 110, 211, 193, 84, 164, 185],
      ?assertEqual(Expectation,
                   rabbitmq_aws_sign:signing_key(SecretKey, AMZDate, Region, Service))
     end}
  ].

string_to_sign_test_() ->
  [
    {"string value", fun() ->
      RequestTimestamp = "20150830T123600Z",
      RequestDate = "20150830",
      Region = "us-east-1",
      Service = "iam",
      RequestHash = "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59",
      Expectation = "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\nf536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59",
      ?assertEqual(Expectation,
                   rabbitmq_aws_sign:string_to_sign(RequestTimestamp, RequestDate, Region, Service, RequestHash))
     end}
  ].

local_time_0_test_() ->
  {foreach,
    fun() ->
      meck:new(calendar, [passthrough, unstick])
    end,
    fun(_) ->
      meck:unload(calendar)
    end,
    [
      {"variation1", fun() ->
        meck:expect(calendar, local_time_to_universal_time_dst, fun(_) -> [{{2015, 05, 08}, {12, 36, 00}}] end),
        Expectation = "20150508T123600Z",
        ?assertEqual(Expectation, rabbitmq_aws_sign:local_time()),
        meck:validate(calendar)
                     end}
    ]}.

local_time_1_test_() ->
  [
    {"variation1", fun() ->
      Value = {{2015, 05, 08}, {13, 15, 20}},
      Expectation = "20150508T131520Z",
      ?assertEqual(Expectation, rabbitmq_aws_sign:local_time(Value))
                   end},
    {"variation2", fun() ->
      Value = {{2015, 05, 08}, {06, 07, 08}},
      Expectation = "20150508T060708Z",
      ?assertEqual(Expectation, rabbitmq_aws_sign:local_time(Value))
                   end}
  ].

headers_test_() ->
  {foreach,
    fun() ->
      meck:new(calendar, [passthrough, unstick])
    end,
    fun(_) ->
      meck:unload(calendar)
    end,
    [
      {"without signing key", fun() ->
        meck:expect(calendar, local_time_to_universal_time_dst, fun(_) -> [{{2015, 08, 30}, {12, 36, 00}}] end),
        Request = #request{
          access_key = "AKIDEXAMPLE",
          secret_access_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
          service = "iam",
          method = get,
          region = "us-east-1",
          uri = "https://iam.amazonaws.com/?Action=ListUsers&Version=2015-05-08",
          body = "",
          headers = [{"Content-Type", "application/x-www-form-urlencoded; charset=utf-8"}]},
        Expectation = [
          {"authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-length;content-type;date;host;x-amz-content-sha256, Signature=81cb49e1e232a0a5f7f594ad6b2ad2b8b7adbafddb3604d00491fe8f3cc5a442"},
          {"content-length", "0"},
          {"content-type", "application/x-www-form-urlencoded; charset=utf-8"},
          {"date", "20150830T123600Z"},
          {"host", "iam.amazonaws.com"},
          {"x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
        ],
        ?assertEqual(Expectation, rabbitmq_aws_sign:headers(Request)),
        meck:validate(calendar)
       end},
      {"with host header", fun() ->
        meck:expect(calendar, local_time_to_universal_time_dst, fun(_) -> [{{2015, 08, 30}, {12, 36, 00}}] end),
        Request = #request{
          access_key = "AKIDEXAMPLE",
          secret_access_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
          service = "iam",
          method = get,
          region = "us-east-1",
          uri = "https://s3.us-east-1.amazonaws.com/?list-type=2",
          body = "",
          headers = [{"host", "gavinroy.com.s3.amazonaws.com"}]},
        Expectation = [
          {"authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-length;date;host;x-amz-content-sha256, Signature=64e549daad14fc1ba9fc4aca6b7df4b2c60e352e3313090d84a2941c1e653d36"},
          {"content-length","0"},
          {"date","20150830T123600Z"},
          {"host","gavinroy.com.s3.amazonaws.com"},
          {"x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
        ],
        ?assertEqual(Expectation, rabbitmq_aws_sign:headers(Request)),
        meck:validate(calendar)
      end}
    ]
  }.
