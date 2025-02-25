var container = require('rhea')  // https://github.com/amqp/rhea
var fs = require('fs');
var path = require('path');
var connectionOptions = getConnectionOptions()

function getAmqpConnectionOptions() {
  return {
    'host': process.env.RABBITMQ_HOSTNAME || 'rabbitmq',
    'port': process.env.RABBITMQ_AMQP_PORT || 5672,
    'username' : process.env.RABBITMQ_AMQP_USERNAME || 'guest',
    'password' : process.env.RABBITMQ_AMQP_PASSWORD || 'guest',
    'id': "selenium-connection-id",
    'container_id': "selenium-container-id"        
  }
}
function getAmqpsConnectionOptions() {
  let options = getAmqpConnectionOptions()
  let useMtls = process.env.AMQP_USE_MTLS || false
  if (useMtls) {
    options['enable_sasl_external'] = true  
  }
  options['transport'] = 'tls'
  let certsLocation = process.env.RABBITMQ_CERTS
  options['key'] = fs.readFileSync(path.resolve(certsLocation,'client_rabbitmq_key.pem'))
  options['cert'] = fs.readFileSync(path.resolve(certsLocation,'client_rabbitmq_certificate.pem'))
  options['ca'] = fs.readFileSync(path.resolve(certsLocation,'ca_rabbitmq_certificate.pem')) 
  return options
}
function getConnectionOptions() {
  let scheme = process.env.RABBITMQ_AMQP_SCHEME || 'amqp'
  console.log("Using AMQP protocol: " + scheme)
  switch(scheme){
    case "amqp":
      return getAmqpConnectionOptions()
    case "amqps":
      return getAmqpsConnectionOptions()
  }  
}
module.exports = {  
  
  open: () => {
    let promise = new Promise((resolve, reject) => {
      container.on('connection_open', function(context) {
        resolve()
      })
    })
    let connection = container.connect(connectionOptions)
    let receiver = connection.open_receiver({
      source: 'my-queue',
      target: 'receiver-target',
      name: 'receiver-link'
    })
    let sender = connection.open_sender({
      target: 'my-queue',
      source: 'sender-source',
      name: 'sender-link'
    })
    return {
      'connection': connection,
      'promise' : promise,
      'receiver' : receiver,
      'sender' : sender
    }
  },
  close: (connection) => {
    if (connection != null) {
      connection.close()
    }
  },
  once: (event, callback) => {
    container.once(event, callback)
  },
  on: (event, callback) => {
    container.on(event, callback)
  }
}