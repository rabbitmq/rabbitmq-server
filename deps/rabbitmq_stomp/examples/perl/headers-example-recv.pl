#!/usr/bin/perl -w
# subscribe to messages from the queue 'foo'
use Net::Stomp;
my $stomp = Net::Stomp->new({hostname=>'localhost', port=>'61613'});
my $filterval = ($ARGV[0] or "value1");
print "Receiving from amq.headers with header1 = $filterval\n";
$stomp->connect({login=>'guest', passcode=>'guest'});
$stomp->subscribe({'destination'=>'/exchange/amq.headers', 'ack'=>'client',
                   'X-B-header1'=> $filterval});
while (1) {
    my $frame = $stomp->receive_frame;
    print $frame->body . "\n";
    $stomp->ack({frame=>$frame});
    last if $frame->body eq 'QUIT';
}
$stomp->disconnect;
