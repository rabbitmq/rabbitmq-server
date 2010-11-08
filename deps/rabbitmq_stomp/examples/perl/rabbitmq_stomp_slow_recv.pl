#!/usr/bin/perl -w
# subscribe to messages from the queue 'foo'
use Net::Stomp;
my $stomp = Net::Stomp->new({hostname=>'localhost', port=>'61613'});
$stomp->connect({login=>'guest', passcode=>'guest', prefetch=>1});
$stomp->subscribe({'destination'=>'/queue/foo', 'ack'=>'client'});
while (1) {
    my $frame = $stomp->receive_frame;
    print $frame->body . "\n";
    sleep 1;
    $stomp->ack({frame=>$frame});
    last if $frame->body eq 'QUIT';
}
$stomp->disconnect;
