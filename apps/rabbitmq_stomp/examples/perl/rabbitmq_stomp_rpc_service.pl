#!/usr/bin/perl -w

use Net::Stomp;

my $stomp = Net::Stomp->new({hostname=>'localhost', port=>'61613'});
$stomp->connect({login=>'guest', passcode=>'guest'});

$stomp->subscribe({'destination'=>'/queue/rabbitmq_stomp_rpc_service', 'ack'=>'client'});
while (1) {
    print "Waiting for request...\n";
    my $frame = $stomp->receive_frame;
    print "Received message, reply_to = " . $frame->headers->{"reply-to"} . "\n";
    print $frame->body . "\n";

    $stomp->send({destination => $frame->headers->{"reply-to"}, bytes_message => 1,
                  body => "Got body: " . $frame->body});
    $stomp->ack({frame=>$frame});
    last if $frame->body eq 'QUIT';
}

$stomp->disconnect;
