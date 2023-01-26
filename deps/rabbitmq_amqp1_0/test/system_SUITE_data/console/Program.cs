using Amqp;
using Amqp.Framing;

// See https://aka.ms/new-console-template for more information

// Create the AMQP connection string
var connectionString = $"amqp://guest:guest@localhost:5672/%2F";

// Create the AMQP connection
var connection = new Connection(new Address(connectionString));


for (var i = 0; i < 10; i++)
{
    Session[] sessions = new Session[5];
    SenderLink[] senders = new SenderLink[5];

    for (var j = 0; j < 5; j++)
    {
      // Create the AMQP session
      sessions[j] = new Session(connection);

        // Give a name to the sender
      var senderSubscriptionId = "rabbitmq.amqp.sender";

      // Name of the topic you will be sending messages (Name of the Queue)
      var topic = "amqp10-queue";

      // Create the AMQP sender
      senders[j] = new SenderLink(sessions[j], senderSubscriptionId, topic);

      // Create message
      var message = new Message($"Received message {i}");

      // Add a meesage id
      message.Properties = new Properties() { MessageId = Guid.NewGuid().ToString() };

      // Add some message properties
      message.ApplicationProperties = new ApplicationProperties();
      message.ApplicationProperties["Message.Type.FullName"] = typeof(string).FullName;

      // Send message
      senders[j].Send(message);
      Console.WriteLine("Sent message " + (i+1) + "/10 to amqp10-queue from session " + j + ". Press a button to send next message");
    }
     // Wait for a key to close last sessions
    Console.Read();

    for (var j = 0; j < 5; j++)
    {
      senders[j].Close();
      sessions[j].Close();
    }
}

Console.WriteLine("Sent all messages. Press a button to close the connection");
connection.Close();
