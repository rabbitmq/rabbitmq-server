// vim:sw=4:et:

using NUnit.Framework;

using System;
using System.Linq;
using System.Threading;

using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.Amqp10
{
    [TestFixture]
    public class Testsuite
    {
        [Test]
        public void roundtrip()
        {
            string uri = get_broker_uri();

            Connection connection = new Connection(new Address(uri));
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session,
              "test-sender", "roundtrip-q");

            Message message1 = new Message("Testing roundtrip");
            sender.Send(message1);

            ReceiverLink receiver = new ReceiverLink(session,
              "test-receiver", "roundtrip-q");
            receiver.SetCredit(100, true);
            Message message2 = receiver.Receive();
            receiver.Accept(message2);

            sender.Close();
            receiver.Close();
            session.Close();
            connection.Close();

            Assert.That(message2.GetBody<string>(),
              Is.EqualTo(message1.GetBody<string>()));
        }

        [TestCase("amqp:accepted:list", null)]
        [TestCase("amqp:rejected:list", null)]
        [TestCase("amqp:released:list", null)]
        public void default_outcome(string default_outcome, string condition)
        {
            Outcome default_outcome_obj = null;
            switch (default_outcome) {
                case "amqp:accepted:list":
                           default_outcome_obj = new Accepted();
                           break;
                case "amqp:rejected:list":
                           default_outcome_obj = new Rejected();
                           break;
                case "amqp:released:list":
                           default_outcome_obj = new Released();
                           break;
                case "amqp:modified:list":
                           default_outcome_obj = new Modified();
                           break;
            }

            Attach attach = new Attach() {
                Source = new Source() {
                  Address = "default_outcome-q",
                  DefaultOutcome = default_outcome_obj
                },
                Target = new Target()
              };

            do_test_outcomes(attach, condition);
        }

        [TestCase("amqp:accepted:list", null)]
        [TestCase("amqp:rejected:list", null)]
        [TestCase("amqp:released:list", null)]
        [TestCase("amqp:modified:list", "amqp:not-implemented")]
        public void outcomes(string outcome, string condition)
        {
            Attach attach = new Attach() {
                Source = new Source() {
                  Address = "outcomes-q",
                  Outcomes = new Symbol[] { new Symbol(outcome) }
                },
                Target = new Target()
              };

            do_test_outcomes(attach, condition);
        }

        internal void do_test_outcomes(Attach attach, string condition)
        {
            string uri = get_broker_uri();

            Connection connection = new Connection(new Address(uri));
            Session session = new Session(connection);

            ManualResetEvent mre = new ManualResetEvent(false);
            string error_name = null;

            OnAttached attached = (Link link, Attach _attach) => {
                error_name = null;
                mre.Set();
            };

            ClosedCallback closed = (AmqpObject amqp_obj, Error error) => {
                error_name = error.Condition;
                mre.Set();
            };
            session.Closed = closed;

            ReceiverLink receiver = new ReceiverLink(session,
              "test-receiver", attach, attached);

            mre.WaitOne();
            if (condition == null) {
                Assert.That(error_name, Is.Null);

                session.Closed = null;
                receiver.Close();
                session.Close();
            } else {
                Assert.That(error_name, Is.EqualTo(condition));
            }

            connection.Close();
        }

        [TestCase(512U, 512U)]
        [TestCase(512U, 600U)]
        [TestCase(512U, 1024U)]
        [TestCase(1024U, 1024U)]
        public void fragmentation(uint frame_size, uint payload_size)
        {
            string uri = get_broker_uri();
            Address address = new Address(uri);

            Open open = new Open()
            {
                ContainerId = Guid.NewGuid().ToString(),
                HostName = address.Host,
                ChannelMax = 256,
                MaxFrameSize = frame_size
            };

            Connection connection = new Connection(address, null, open, null);
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session,
              "test-sender", "fragmentation-q");

            Message message1 = new Message(String.Concat(
              Enumerable.Repeat("a", (int)payload_size)));
            sender.Send(message1);

            ReceiverLink receiver = new ReceiverLink(session,
              "test-receiver", "fragmentation-q");
            receiver.SetCredit(100, true);
            Message message2 = receiver.Receive();
            receiver.Accept(message2);

            sender.Close();
            receiver.Close();
            session.Close();
            connection.Close();

            Assert.That(message2.GetBody<string>(),
              Is.EqualTo(message1.GetBody<string>()));
        }

        [Test]
        public void message_annotations()
        {
            string uri = get_broker_uri();

            Connection connection = new Connection(new Address(uri));
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session,
              "test-sender", "annotations-q");

            Message message1 = new Message("Testing message annotations");
            message1.MessageAnnotations = new MessageAnnotations();
            message1.MessageAnnotations[new Symbol("key1")] = "value1";
            message1.MessageAnnotations[new Symbol("key2")] = "value2";
            sender.Send(message1);

            ReceiverLink receiver = new ReceiverLink(session,
              "test-receiver", "annotations-q");
            receiver.SetCredit(100, true);
            Message message2 = receiver.Receive();
            receiver.Accept(message2);

            sender.Close();
            receiver.Close();
            session.Close();
            connection.Close();

            Assert.That(message2.GetBody<string>(),
              Is.EqualTo(message1.GetBody<string>()));
            Assert.That(message2.MessageAnnotations.Descriptor,
              Is.EqualTo(message1.MessageAnnotations.Descriptor));
            Assert.That(message2.MessageAnnotations.Map,
              Is.EqualTo(message1.MessageAnnotations.Map));
        }

        [Test]
        public void footer()
        {
            string uri = get_broker_uri();

            Connection connection = new Connection(new Address(uri));
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session,
              "test-sender", "footer-q");

            Message message1 = new Message("Testing footer");
            message1.Footer = new Footer();
            message1.Footer[new Symbol("key1")] = "value1";
            message1.Footer[new Symbol("key2")] = "value2";
            sender.Send(message1);

            ReceiverLink receiver = new ReceiverLink(session,
              "test-receiver", "footer-q");
            receiver.SetCredit(100, true);
            Message message2 = receiver.Receive();
            receiver.Accept(message2);

            sender.Close();
            receiver.Close();
            session.Close();
            connection.Close();

            Assert.That(message2.GetBody<string>(),
              Is.EqualTo(message1.GetBody<string>()));
            Assert.That(message2.Footer.Descriptor,
              Is.EqualTo(message1.Footer.Descriptor));
            Assert.That(message2.Footer.Map,
              Is.EqualTo(message1.Footer.Map));
        }

        [Test]
        public void data_types()
        {
            string uri = get_broker_uri();

            Connection connection = new Connection(new Address(uri));
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session,
              "test-sender", "data_types-q");

            var list = new Amqp.Types.List();
            list.Add(true);
            list.Add('$');
            list.Add(Byte.MaxValue);
            list.Add(Int16.MinValue);
            list.Add(Int32.MinValue);
            list.Add(Int64.MinValue);
            list.Add(UInt16.MaxValue);
            list.Add(UInt32.MaxValue);
            list.Add(UInt64.MaxValue);
            list.Add(Double.NaN);
            list.Add(null);
            list.Add("\uFFF9");
            list.Add(new Symbol("Symbol"));
            list.Add(DateTime.Parse("2008-11-01T19:35:00.0000000Z").ToUniversalTime());
            list.Add(new Guid("f275ea5e-0c57-4ad7-b11a-b20c563d3b71"));
            list.Add(new Amqp.Types.List() { "Boolean", true });
            list.Add(new AmqpSequence() {
              List = new Amqp.Types.List() { "Integer", 1 }
            });

            AmqpSequence body = new AmqpSequence() { List = list };
            Message message1 = new Message(body);
            sender.Send(message1);

            ReceiverLink receiver = new ReceiverLink(session,
              "test-receiver", "data_types-q");
            receiver.SetCredit(100, true);
            Message message2 = receiver.Receive();
            receiver.Accept(message2);

            sender.Close();
            receiver.Close();
            session.Close();
            connection.Close();

            /* AmqpSequence apparently can't be compared directly: we must
             * compare their list instead. */
            var list1 = message1.GetBody<AmqpSequence>().List;
            var list2 = message2.GetBody<AmqpSequence>().List;
            Assert.That(list2.Count, Is.EqualTo(list1.Count));

            for (int i = 0; i < list1.Count; ++i) {
                if (list[i] != null &&
                  list1[i].GetType() == typeof(AmqpSequence)) {
                    Assert.That(
                      ((AmqpSequence)list2[i]).List,
                      Is.EqualTo(((AmqpSequence)list1[i]).List));
                } else {
                    Assert.That(list2[i], Is.EqualTo(list1[i]));
                }
            }
        }

        [Test]
        public void reject()
        {
            string uri = get_broker_uri();

            Connection connection = new Connection(new Address(uri));
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session,
              "test-sender", "reject-q");

            Message message1 = new Message("Testing reject");
            sender.Send(message1);
            sender.Close();

            ReceiverLink receiver = new ReceiverLink(session,
              "test-receiver", "reject-q");
            receiver.SetCredit(100, true);
            Message message2 = receiver.Receive();
            receiver.Reject(message2);

            Assert.That(receiver.Receive(100), Is.Null);

            receiver.Close();
            session.Close();
            connection.Close();
        }

        [Test]
        public void redelivery()
        {
            string uri = get_broker_uri();

            Connection connection = new Connection(new Address(uri));
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session,
              "test-sender", "redelivery-q");

            Message message1 = new Message("Testing redelivery");
            sender.Send(message1);
            sender.Close();

            ReceiverLink receiver = new ReceiverLink(session,
              "test-receiver", "redelivery-q");
            receiver.SetCredit(100, true);
            Message message2 = receiver.Receive();

            Assert.That(message2.Header.FirstAcquirer, Is.True);
            // FIXME Assert.That(message2.Delivery.Settled, Is.False);

            receiver.Close();
            session.Close();

            session = new Session(connection);
            receiver = new ReceiverLink(session,
              "test-receiver", "redelivery-q");
            receiver.SetCredit(100, true);
            Message message3 = receiver.Receive();
            receiver.Accept(message3);

            Assert.That(message3.GetBody<string>(),
              Is.EqualTo(message2.GetBody<string>()));
            Assert.That(message3.Header.FirstAcquirer, Is.False);

            Assert.That(receiver.Receive(100), Is.Null);

            receiver.Close();
            session.Close();
            connection.Close();
        }

        [TestCase("/queue/test", "test", "", true)]
        [TestCase("test", "/queue/test", "", true)]
        [TestCase("test", "test", "", true)]

        [TestCase("/topic/a.b.c.d",      "/topic/#.c.*",              "",        true)]
        [TestCase("/exchange/amq.topic", "/topic/#.c.*",              "a.b.c.d", true)]
        [TestCase("/topic/w.x.y.z",      "/exchange/amq.topic/#.y.*", "",        true)]
        [TestCase("/exchange/amq.topic", "/exchange/amq.topic/#.y.*", "w.x.y.z", true)]

        [TestCase("/exchange/amq.fanout",  "/exchange/amq.fanout/",  "",  true)]
        [TestCase("/exchange/amq.direct",  "/exchange/amq.direct/",  "",  true)]
        [TestCase("/exchange/amq.direct",  "/exchange/amq.direct/a", "a", true)]

        /* FIXME: The following three tests rely on the queue "test"
         * created by previous tests in this function. */
        [TestCase("/queue/test",     "/amq/queue/test", "", true)]
        [TestCase("/amq/queue/test", "/queue/test",     "", true)]
        [TestCase("/amq/queue/test", "/amq/queue/test", "", true)]

        /* The following tests verify that a queue created out-of-band
         * in AMQP is reachable from the AMQP 1.0 world. Queues are created
         * from the common_test suite. */
        [TestCase("/amq/queue/transient_q", "/amq/queue/transient_q", "", true)]
        [TestCase("/amq/queue/durable_q",   "/amq/queue/durable_q",   "", true)]
        [TestCase("/amq/queue/autodel_q",   "/amq/queue/autodel_q",   "", true)]

        public void routing(String target, String source,
          String routing_key, bool succeed)
        {
            string uri = get_broker_uri();

            Connection connection = new Connection(new Address(uri));
            Session session = new Session(connection);
            SenderLink sender = new SenderLink(session,
              "test-sender", target);
            ReceiverLink receiver = new ReceiverLink(session,
              "test-receiver", source);
            receiver.SetCredit(100, true);

            Random rnd = new Random();
            Message message1 = new Message(rnd.Next(10000));
            Properties props = new Properties() {
                Subject = routing_key
            };
            message1.Properties = props;
            sender.Send(message1);

            if (succeed) {
                Message message2 = receiver.Receive(3000);
                receiver.Accept(message2);
                Assert.That(message2, Is.Not.Null);
                Assert.That(message2.GetBody<int>(),
                  Is.EqualTo(message1.GetBody<int>()));
            } else {
                Message message2 = receiver.Receive(100);
                Assert.That(message2, Is.Null);
            }

            sender.Close();
            receiver.Close();
            session.Close();
            connection.Close();
        }

        [TestCase("/exchange/missing", "amqp:not-found")]
        [TestCase("/fruit/orange", "amqp:invalid-field")]
        public void invalid_routes(string dest, string condition)
        {
            string uri = get_broker_uri();

            Connection connection = new Connection(new Address(uri));
            Session session = new Session(connection);

            ManualResetEvent mre = new ManualResetEvent(false);
            string error_name = null;

            OnAttached attached = delegate(Link link, Attach attach) {
                mre.Set();
            };

            ClosedCallback closed = (AmqpObject amqp_obj, Error error) => {
                error_name = error.Condition;
                mre.Set();
            };
            session.Closed = closed;

            SenderLink sender = new SenderLink(session,
              "test-sender", new Target() { Address = dest }, attached);
            mre.WaitOne();
            Assert.That(error_name, Is.EqualTo(condition));

            error_name = null;
            mre.Reset();

            Assert.That(
              () => {
                  ReceiverLink receiver = new ReceiverLink(session,
                    "test-receiver", dest);
                  receiver.Close();
              },
              Throws.TypeOf<Amqp.AmqpException>()
              .With.Property("Error")
              .With.Property("Condition").EqualTo(new Symbol(condition)));

            session.Closed = null;
            session.Close();
            connection.Close();
        }

        internal string get_broker_uri()
        {
            TestParameters parameters = TestContext.Parameters;
            string uri = parameters["rmq_broker_uri"];
            if (uri == null)
                uri =
                  System.Environment.GetEnvironmentVariable("RMQ_BROKER_URI");
            Assert.That(uri, Is.Not.Null);

            return uri;
        }
    }
}
