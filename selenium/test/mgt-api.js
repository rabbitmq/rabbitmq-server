const XMLHttpRequest = require('xmlhttprequest').XMLHttpRequest
const { escapeCss } = require('selenium-webdriver')
const {log, error} = require('./utils.js')

const baseUrl = randomly_pick_baseurl(process.env.RABBITMQ_URL || 'http://localhost:15672/')
const otherBaseUrl = randomly_pick_baseurl(process.env.OTHER_RABBITMQ_URL || 'http://localhost:15675/')
const hostname = process.env.RABBITMQ_HOSTNAME || 'localhost'
const otherHostname = process.env.OTHER_RABBITMQ_HOSTNAME || 'localhost'

function randomly_pick_baseurl (baseUrl) {
  urls = baseUrl.split(",")
  return urls[getRandomInt(urls.length)]
}
function getRandomInt(max) {
  return Math.floor(Math.random() * max)
}

module.exports = {
 
  getManagementUrl: () => {
    return baseUrl
  },

  getOtherManagementUrl: () => {
    return otherBaseUrl
  },
  basicAuthorization: (username, password) => {
    return "Basic " + btoa(username + ":" + password)
  },
  publish: (url, authorization, vhost, exchange, routingKey, payload) => {
    const req = new XMLHttpRequest()
    
    let body = {
      "properties" : {},
      "routing_key" : routingKey,
      "payload" : payload,
      "payload_encoding" : "string"
    }
    log("Publish message to vhost " + vhost + " with exchnage " + exchange + " : " + JSON.stringify(body))
    
    let finalUrl = url + "/api/exchanges/" + encodeURIComponent(vhost) + "/" +
      encodeURIComponent(exchange) + "/publish" 
    req.open('POST', finalUrl, false)
    req.setRequestHeader("Authorization", authorization)
    req.setRequestHeader('Content-Type', 'application/json')
    
    req.send(JSON.stringify(body))
    if (req.status == 200 || req.status == 204 || req.status == 201) {
      log("Succesfully published message")
      return
    }else {
      error("status:" + req.status + " : " + req.responseText)
      throw new Error(req.responseText)
    }
  },
  getNodes: (url) => {
    log("Getting rabbitmq nodes ...")
    const req = new XMLHttpRequest()
    let base64Credentials = btoa('administrator-only' + ":" + 'guest')
    let finalUrl = url + "/api/nodes?columns=name"

    req.open('GET', finalUrl, false)
    req.setRequestHeader("Authorization", "Basic " + base64Credentials)
    
    req.send()
    if (req.status == 200 || req.status == 204 || req.status == 201) {
      log("Succesfully got nodes ")
      return JSON.parse(req.responseText)
    }else {
      error("status:" + req.status + " : " + req.responseText)
      throw new Error(req.responseText)
    }
  },
  setPolicy: (url, vhost, name, pattern, definition, appliedTo = "queues") => {
    let policy = {
      "pattern": pattern,
      "apply-to": appliedTo,
      "definition": definition
    }
    log("Setting policy " + JSON.stringify(policy) 
      + " with name " + name + " for vhost " + vhost + " on "+ url)
    const req = new XMLHttpRequest()
    let base64Credentials = btoa('administrator-only' + ":" + 'guest')
    let finalUrl = url + "/api/policies/" + encodeURIComponent(vhost) + "/" +
      encodeURIComponent(name)
    req.open('PUT', finalUrl, false)
    req.setRequestHeader("Authorization", "Basic " + base64Credentials)
    req.setRequestHeader('Content-Type', 'application/json')
    
    req.send(JSON.stringify(policy))
    if (req.status == 200 || req.status == 204 || req.status == 201) {
      log("Succesfully set policy " + name)
      return
    }else {
      error("status:" + req.status + " : " + req.responseText)
      throw new Error(req.responseText)
    }
  },
  deletePolicy: (url, vhost, name) => {
    log("Deleting policy " + name + " on vhost " + vhost)
    const req = new XMLHttpRequest()
    let base64Credentials = btoa('administrator-only' + ":" + 'guest')
    let finalUrl = url + "/api/policies/" + encodeURIComponent(vhost) + "/" + 
      encodeURIComponent(name)
    req.open('DELETE', finalUrl, false)
    req.setRequestHeader("Authorization", "Basic " + base64Credentials)
    
    req.send()
    if (req.status == 200 || req.status == 204) {
        log("Succesfully deleted policy " + name)
        return 
    }else {
      error("status:" + req.status + " : " + req.responseText)
      throw new Error(req.responseText)
    }
  },
  createVhost: (url, authorization, name, description = "", tags = []) => {
    let vhost = {
      "description": description,
      "tags": tags
    }
    log("Create vhost " + JSON.stringify(vhost) 
      + " with name " + name + " on " + url)
    const req = new XMLHttpRequest()
    let finalUrl = url + "/api/vhosts/" + encodeURIComponent(name)
    req.open('PUT', finalUrl, false)
    req.setRequestHeader("Authorization", authorization)
    req.setRequestHeader('Content-Type', 'application/json')
    
    req.send(JSON.stringify(vhost))
    if (req.status == 200 || req.status == 204 || req.status == 201) {
      log("Succesfully created vhost " + name)
      return
    }else {
      error("status:" + req.status + " : " + req.responseText)
      throw new Error(req.responseText)
    }
  },
  grantPermissions: (url, vhost, user, permissions) => {
    log("Granting permissions [" + JSON.stringify(permissions) + 
      "] for user " + user + " on vhost " + vhost + " on " + url)

    const req = new XMLHttpRequest()
    let base64Credentials = btoa('administrator-only' + ":" + 'guest')
    let finalUrl = url + "/api/permissions/" + encodeURIComponent(vhost) + "/"
      + encodeURIComponent(user)
    req.open('PUT', finalUrl, false)
    req.setRequestHeader("Authorization", "Basic " + base64Credentials)
    req.setRequestHeader('Content-Type', 'application/json')
    
    req.send(JSON.stringify(permissions))
    if (req.status == 200 || req.status == 204 || req.status == 201) {
      log("Succesfully granted permissions")
      return
    }else {
      error("status:" + req.status + " : " + req.responseText)
      throw new Error(req.responseText)
    }
  },
  deleteVhost: (url, authorization, vhost) => {
    log("Deleting vhost " + vhost)
    const req = new XMLHttpRequest()
    let finalUrl = url + "/api/vhosts/" + encodeURIComponent(vhost)
    req.open('DELETE', finalUrl, false)
    req.setRequestHeader("Authorization", authorization)
    
    req.send()
    if (req.status == 200 || req.status == 204) {
        log("Succesfully deleted vhost " + vhost)
        return 
    }else {
      error("status:" + req.status + " : " + req.responseText)
      throw new Error(req.responseText)
    }
  },
  getQueue: (url, name, vhost) => {
    log("Getting queue " + name + " on vhost " + vhost)
    const req = new XMLHttpRequest()
    let base64Credentials = btoa('administrator-only' + ":" + 'guest')
    let finalUrl = url + "/api/queues/" + encodeURIComponent(vhost) + "/" + 
      encodeURIComponent(name)

    req.open('GET', finalUrl, false)
    req.setRequestHeader("Authorization", "Basic " + base64Credentials)
    
    req.send()
    if (req.status == 200 || req.status == 204 || req.status == 201) {
      log("Succesfully got queue ")
      return JSON.parse(req.responseText)
    }else {
      error("status:" + req.status + " : " + req.responseText)
      throw new Error(req.responseText)
    }
  },
  createQueue: (url, authorization, vhost, name, arguments = {}) => {    
    log("Create queue " + JSON.stringify(name) 
      + " in vhost " + vhost + " on " + url)
    const req = new XMLHttpRequest()
    let finalUrl = url + "/api/queues/" + encodeURIComponent(vhost) + "/"
      + encodeURIComponent(name)
    req.open('PUT', finalUrl, false)
    req.setRequestHeader("Authorization", authorization)
    req.setRequestHeader('Content-Type', 'application/json')
    let payload = {
      "durable": true,
      "arguments": arguments
    }
    req.send(JSON.stringify(payload))
    if (req.status == 200 || req.status == 204 || req.status == 201) {
      log("Succesfully created queue " + name)
      return
    }else {
      error("status:" + req.status + " : " + req.responseText)
      throw new Error(req.responseText)
    }
  },
  deleteQueue: (url, authorization, vhost, name) => {
    log("Deleting queue " + name + " on vhost " + vhost)
    const req = new XMLHttpRequest()
    let finalUrl = url + "/api/queues/" + encodeURIComponent(vhost) + "/"
      + encodeURIComponent(name)
    req.open('DELETE', finalUrl, false)
    req.setRequestHeader("Authorization", authorization)
    
    req.send()
    if (req.status == 200 || req.status == 204) {
        log("Succesfully deleted queue " + vhost)
        return 
    }else {
      error("status:" + req.status + " : " + req.responseText)
      throw new Error(req.responseText)
    }
  }

}
