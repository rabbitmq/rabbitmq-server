#!/usr/bin/perl -w
# send a message to the queue 'foo'
use Net::Stomp;
my $stomp = Net::Stomp->new({hostname=>'localhost', port=>'61613'});
$stomp->connect({login=>'guest', passcode=>'guest'});
for (my $i = 0; $i < 10000; $i++) {
    $stomp->send({destination=>'/queue/foo',
		  bytes_message=>1,
		  body=>($ARGV[0] or "message $i")});
}
$stomp->disconnect;
