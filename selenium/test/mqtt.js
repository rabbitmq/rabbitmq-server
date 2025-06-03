const mqtt = require('mqtt')

module.exports = {  

  openConnection: (mqttOptions) => {
    let rabbit = process.env.RABBITMQ_HOSTNAME || 'localhost'
    let mqttUrl = process.env.RABBITMQ_MQTT_URL || "mqtt://" + rabbit + ":1883"
    return mqtt.connect(mqttUrl, mqttOptions)
  },
  getConnectionOptions: () => {
    let mqttProtocol = process.env.MQTT_PROTOCOL || 'mqtt'
    let usemtls = process.env.MQTT_USE_MTLS || false
    let username = process.env.RABBITMQ_AMQP_USERNAME || 'management'
    let password = process.env.RABBITMQ_AMQP_PASSWORD || 'guest'
    let client_id = process.env.RABBITMQ_AMQP_USERNAME || 'selenium-client'

    mqttOptions = {
      clientId: client_id,
      protocolId: 'MQTT',
      protocol: mqttProtocol,
      protocolVersion: 5,
      keepalive: 10000,
      clean: false,
      reconnectPeriod: '1000',
      properties: {
          sessionExpiryInterval: 0 
      }
    }

    if (mqttProtocol == 'mqtts') {
      mqttOptions["ca"] = [fs.readFileSync(process.env.RABBITMQ_CERTS + "/ca_rabbitmq_certificate.pem")]      
    } 
    if (usemtls) {
      mqttOptions["cert"] = fs.readFileSync(process.env.RABBITMQ_CERTS + "/client_rabbitmq_certificate.pem")
      mqttOptions["key"] = fs.readFileSync(process.env.RABBITMQ_CERTS + "/client_rabbitmq_key.pem")
    } else {
      mqttOptions["username"] = username
      mqttOptions["password"] = password
    }
    return mqttOptions
  }
}
