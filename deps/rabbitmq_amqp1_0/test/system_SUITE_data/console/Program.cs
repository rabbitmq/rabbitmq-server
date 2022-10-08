using Amqp;
using Amqp.Framing;

// See https://aka.ms/new-console-template for more information
Console.WriteLine("Sending 10 messages to amqp10-queue. Press a button to terminate");

// Create the AMQP connection string
var connectionString = $"amqp://guest:guest@localhost:5672/%2F";

// Create the AMQP connection
var connection = new Connection(new Address(connectionString));

// Create the AMQP session
var amqpSession = new Session(connection);

  // Give a name to the sender
var senderSubscriptionId = "rabbitmq.amqp.sender";

// Name of the topic you will be sending messages (Name of the Queue)
var topic = "amqp10-queue";

// Create the AMQP sender
var sender = new SenderLink(amqpSession, senderSubscriptionId, topic);

for (var i = 0; i < 10; i++)
{
    // Create message
    var message = new Message($"Received message {i}");

    // Add a meesage id
    message.Properties = new Properties() { MessageId = Guid.NewGuid().ToString() };

    // Add some message properties
    message.ApplicationProperties = new ApplicationProperties();
    message.ApplicationProperties["Message.Type.FullName"] = typeof(string).FullName;

    // Send message
    sender.Send(message);

    Task.Delay(2000).Wait();
}

 // Wait for a key to close the program
Console.Read();
