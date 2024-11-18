const { By, Key, until, Builder } = require('selenium-webdriver')
require('chromedriver')
const assert = require('assert')
const { buildDriver, goToHome, captureScreensFor, teardown, delay } = require('../../utils')

const LoginPage = require('../../pageobjects/LoginPage')
const OverviewPage = require('../../pageobjects/OverviewPage')
const ConnectionsPage = require('../../pageobjects/ConnectionsPage')
const ConnectionPage = require('../../pageobjects/ConnectionPage')

var container = require('rhea')  // https://github.com/amqp/rhea
var receivedAmqpMessageCount = 0
var untilConnectionEstablished = new Promise((resolve, reject) => {
  container.on('connection_open', function(context) {
    resolve()
  })
})

container.on('message', function (context) {
    receivedAmqpMessageCount++
})
container.once('sendable', function (context) {
    context.sender.send({body:'first message'})    
})


describe('Given an amqp10 connection opened, listed and clicked on it', function () {  
  let captureScreen
  let connectionsPage
  let connectionPage
  let connection 

  before(async function () {
    driver = buildDriver()
    await goToHome(driver)
    login = new LoginPage(driver)
    overview = new OverviewPage(driver)
    connectionsPage = new ConnectionsPage(driver)
    connectionPage = new ConnectionPage(driver)
    captureScreen = captureScreensFor(driver, __filename)
    await login.login('monitoring-only', 'guest')
    await overview.isLoaded()

    connection = container.connect(
      {'host': process.env.RABBITMQ_HOSTNAME || 'rabbitmq',
       'port': process.env.RABBITMQ_AMQP_PORT || 5672,
       'username' : process.env.RABBITMQ_AMQP_USERNAME || 'guest',
       'password' : process.env.RABBITMQ_AMQP_PASSWORD || 'guest',
       'id': "selenium-connection-id",
       'container_id': "selenium-container-id"
      })
    connection.open_receiver({
      source: 'examples',
      target: 'receiver-target',
      name: 'receiver-link'
    })
    sender = connection.open_sender({
      target: 'examples',
      source: 'sender-source',
      name: 'sender-link'
    })
    await untilConnectionEstablished
    await overview.clickOnConnectionsTab()    
    await connectionsPage.isLoaded()

    connections_table = await connectionsPage.getConnectionsTable(20)
    assert.equal(1, connections_table.length)
    await connectionsPage.clickOnConnection(2)
    await connectionPage.isLoaded()
  })


  it('can list session information', async function () {
    let sessions = await connectionPage.getSessions()
    assert.equal(1, sessions.sessions.length)
    let session = connectionPage.getSessionInfo(sessions.sessions, 0)
    console.log("session: " + JSON.stringify(session))
    assert.equal(0, session.channelNumber)
    assert.equal(1, session.nextIncomingId)
    assert.equal(0, session.outgoingUnsettledDeliveries)
  })
  
  it('can list link information', async function () {
    let sessions = await connectionPage.getSessions()
    assert.equal(1, sessions.incoming_links.length)
    assert.equal(1, sessions.outgoing_links.length)    
    
    let incomingLink = connectionPage.getIncomingLinkInfo(sessions.incoming_links, 0)
    console.log("incomingLink: " + JSON.stringify(incomingLink))
    assert.equal(1, incomingLink.handle)
    assert.equal("sender-link", incomingLink.name)
    assert.equal("examples", incomingLink.targetAddress)
    assert.equal("mixed", incomingLink.sndSettleMode)
    assert.equal("0", incomingLink.unconfirmedMessages)
    assert.equal(1, incomingLink.deliveryCount)

    let outgoingLink = connectionPage.getOutgoingLinkInfo(sessions.outgoing_links, 0)
    console.log("outgoingLink: " + JSON.stringify(outgoingLink))
    assert.equal(0, outgoingLink.handle)
    assert.equal("receiver-link", outgoingLink.name)
    assert.equal("examples", outgoingLink.sourceAddress)
    assert.equal("examples", outgoingLink.queueName)
    
    assert.equal(false, outgoingLink.sendSettled)
    assert.equal("unlimited", outgoingLink.maxMessageSize)
   
  })

  it('display live link information', async function () {
    var untilMessageReceived = new Promise((resolve, reject) => {
      container.on('message', function(context) {
        resolve()
      })
    })
    sender.send({body:'second message'})    
    await untilMessageReceived
    assert.equal(2, receivedAmqpMessageCount)

    await delay(5*1000) // wait until page refreshes
    let sessions = await connectionPage.getSessions()
    let session = connectionPage.getSessionInfo(sessions.sessions, 0)
    let incomingLink = connectionPage.getIncomingLinkInfo(sessions.incoming_links, 0)
    let outgoingLink = connectionPage.getOutgoingLinkInfo(sessions.outgoing_links, 0)
    assert.equal(2, incomingLink.deliveryCount)
    
    //console.log("incomingLink: " + JSON.stringify(incomingLink))
    //console.log("outgoingLink: " + JSON.stringify(outgoingLink))
  }) 


  after(async function () {    
    await teardown(driver, this, captureScreen)
    try {
      if (connection != null) {
        connection.close()
      }
    } catch (error) {
      console.error("Failed to close amqp10 connection due to " + error);      
    }  
  })

})
