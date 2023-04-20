-module(rabbitmq_aws_xml_tests).

-include_lib("eunit/include/eunit.hrl").

-include("rabbitmq_aws.hrl").

parse_test_() ->
  [
    {"s3 error response", fun() ->
      Response = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>SignatureDoesNotMatch</Code><Message>The request signature we calculated does not match the signature you provided. Check your key and signing method.</Message><AWSAccessKeyId>AKIAIPPU25E5RA4MIYKQ</AWSAccessKeyId><StringToSign>AWS4-HMAC-SHA256\n20160516T041429Z\n20160516/us-east-1/s3/aws4_request\n7e908e36ea6c07e542ffac21ec3e11acc3baf022d9133d9764e1521b152586f7</StringToSign><SignatureProvided>841d7b89150d246feee9bceb90f5cae91d0c45f44851742c73eb87dc8472748e</SignatureProvided><StringToSignBytes>41 57 53 34 2d 48 4d 41 43 2d 53 48 41 32 35 36 0a 32 30 31 36 30 35 31 36 54 30 34 31 34 32 39 5a 0a 32 30 31 36 30 35 31 36 2f 75 73 2d 65 61 73 74 2d 31 2f 73 33 2f 61 77 73 34 5f 72 65 71 75 65 73 74 0a 37 65 39 30 38 65 33 36 65 61 36 63 30 37 65 35 34 32 66 66 61 63 32 31 65 63 33 65 31 31 61 63 63 33 62 61 66 30 32 32 64 39 31 33 33 64 39 37 36 34 65 31 35 32 31 62 31 35 32 35 38 36 66 37</StringToSignBytes><CanonicalRequest>GET\n/\nlist-type=2\ncontent-length:0\ndate:20160516T041429Z\nhost:s3.us-east-1.amazonaws.com\nx-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n\ncontent-length;date;host;x-amz-content-sha256\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</CanonicalRequest><CanonicalRequestBytes>47 45 54 0a 2f 0a 6c 69 73 74 2d 74 79 70 65 3d 32 0a 63 6f 6e 74 65 6e 74 2d 6c 65 6e 67 74 68 3a 30 0a 64 61 74 65 3a 32 30 31 36 30 35 31 36 54 30 34 31 34 32 39 5a 0a 68 6f 73 74 3a 73 33 2e 75 73 2d 65 61 73 74 2d 31 2e 61 6d 61 7a 6f 6e 61 77 73 2e 63 6f 6d 0a 78 2d 61 6d 7a 2d 63 6f 6e 74 65 6e 74 2d 73 68 61 32 35 36 3a 65 33 62 30 63 34 34 32 39 38 66 63 31 63 31 34 39 61 66 62 66 34 63 38 39 39 36 66 62 39 32 34 32 37 61 65 34 31 65 34 36 34 39 62 39 33 34 63 61 34 39 35 39 39 31 62 37 38 35 32 62 38 35 35 0a 0a 63 6f 6e 74 65 6e 74 2d 6c 65 6e 67 74 68 3b 64 61 74 65 3b 68 6f 73 74 3b 78 2d 61 6d 7a 2d 63 6f 6e 74 65 6e 74 2d 73 68 61 32 35 36 0a 65 33 62 30 63 34 34 32 39 38 66 63 31 63 31 34 39 61 66 62 66 34 63 38 39 39 36 66 62 39 32 34 32 37 61 65 34 31 65 34 36 34 39 62 39 33 34 63 61 34 39 35 39 39 31 62 37 38 35 32 62 38 35 35</CanonicalRequestBytes><RequestId>8EB36F450B78C45D</RequestId><HostId>IYXsnJ59yqGI/IzjGoPGUz7NGb/t0ETlWH4v5+l8EGWmHLbhB1b2MsjbSaY5A8M3g7Fn/Nliqpw=</HostId></Error>",
      Expectation = [{"Error", [
        {"Code", "SignatureDoesNotMatch"},
        {"Message", "The request signature we calculated does not match the signature you provided. Check your key and signing method."},
        {"AWSAccessKeyId", "AKIAIPPU25E5RA4MIYKQ"},
        {"StringToSign",  "AWS4-HMAC-SHA256\n20160516T041429Z\n20160516/us-east-1/s3/aws4_request\n7e908e36ea6c07e542ffac21ec3e11acc3baf022d9133d9764e1521b152586f7"},
        {"SignatureProvided", "841d7b89150d246feee9bceb90f5cae91d0c45f44851742c73eb87dc8472748e"},
        {"StringToSignBytes", "41 57 53 34 2d 48 4d 41 43 2d 53 48 41 32 35 36 0a 32 30 31 36 30 35 31 36 54 30 34 31 34 32 39 5a 0a 32 30 31 36 30 35 31 36 2f 75 73 2d 65 61 73 74 2d 31 2f 73 33 2f 61 77 73 34 5f 72 65 71 75 65 73 74 0a 37 65 39 30 38 65 33 36 65 61 36 63 30 37 65 35 34 32 66 66 61 63 32 31 65 63 33 65 31 31 61 63 63 33 62 61 66 30 32 32 64 39 31 33 33 64 39 37 36 34 65 31 35 32 31 62 31 35 32 35 38 36 66 37"},
        {"CanonicalRequest",  "GET\n/\nlist-type=2\ncontent-length:0\ndate:20160516T041429Z\nhost:s3.us-east-1.amazonaws.com\nx-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n\ncontent-length;date;host;x-amz-content-sha256\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
        {"CanonicalRequestBytes", "47 45 54 0a 2f 0a 6c 69 73 74 2d 74 79 70 65 3d 32 0a 63 6f 6e 74 65 6e 74 2d 6c 65 6e 67 74 68 3a 30 0a 64 61 74 65 3a 32 30 31 36 30 35 31 36 54 30 34 31 34 32 39 5a 0a 68 6f 73 74 3a 73 33 2e 75 73 2d 65 61 73 74 2d 31 2e 61 6d 61 7a 6f 6e 61 77 73 2e 63 6f 6d 0a 78 2d 61 6d 7a 2d 63 6f 6e 74 65 6e 74 2d 73 68 61 32 35 36 3a 65 33 62 30 63 34 34 32 39 38 66 63 31 63 31 34 39 61 66 62 66 34 63 38 39 39 36 66 62 39 32 34 32 37 61 65 34 31 65 34 36 34 39 62 39 33 34 63 61 34 39 35 39 39 31 62 37 38 35 32 62 38 35 35 0a 0a 63 6f 6e 74 65 6e 74 2d 6c 65 6e 67 74 68 3b 64 61 74 65 3b 68 6f 73 74 3b 78 2d 61 6d 7a 2d 63 6f 6e 74 65 6e 74 2d 73 68 61 32 35 36 0a 65 33 62 30 63 34 34 32 39 38 66 63 31 63 31 34 39 61 66 62 66 34 63 38 39 39 36 66 62 39 32 34 32 37 61 65 34 31 65 34 36 34 39 62 39 33 34 63 61 34 39 35 39 39 31 62 37 38 35 32 62 38 35 35"},
        {"RequestId","8EB36F450B78C45D"},
        {"HostId", "IYXsnJ59yqGI/IzjGoPGUz7NGb/t0ETlWH4v5+l8EGWmHLbhB1b2MsjbSaY5A8M3g7Fn/Nliqpw="}
      ]}],
      ?assertEqual(Expectation, rabbitmq_aws_xml:parse(Response))
     end},
    {"whitespace", fun() ->
      Response = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<test>  <example> value</example>\n</test>  \n",
      Expectation = [{"test", [{"example", "value"}]}],
      ?assertEqual(Expectation, rabbitmq_aws_xml:parse(Response))
     end},
    {"multiple items", fun() ->
      Response = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<test><values><example>value</example><example>value2</example></values>\n</test>  \n",
      Expectation = [{"test", [{"values", [{"example", "value"}, {"example", "value2"}]}]}],
      ?assertEqual(Expectation, rabbitmq_aws_xml:parse(Response))
     end},
    {"small snippert", fun() ->
      Response = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<test>value</test>",
      Expectation = [{"test", "value"}],
      ?assertEqual(Expectation, rabbitmq_aws_xml:parse(Response))
     end}
  ].
