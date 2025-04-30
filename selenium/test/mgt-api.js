const XMLHttpRequest = require('xmlhttprequest').XMLHttpRequest
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

  geOtherManagementUrl: () => {
    return otherBaseUrl
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
  createVhost: (url, name, description = "", tags = []) => {
    let vhost = {
      "description": description,
      "tags": tags
    }
    log("Create vhost " + JSON.stringify(vhost) 
      + " with name " + name + " on " + url)
    const req = new XMLHttpRequest()
    let base64Credentials = btoa('administrator-only' + ":" + 'guest')
    let finalUrl = url + "/api/vhosts/" + encodeURIComponent(name)
    req.open('PUT', finalUrl, false)
    req.setRequestHeader("Authorization", "Basic " + base64Credentials)
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
  deleteVhost: (url, vhost) => {
    log("Deleting vhost " + vhost)
    const req = new XMLHttpRequest()
    let base64Credentials = btoa('administrator-only' + ":" + 'guest')
    let finalUrl = url + "/api/vhosts/" + encodeURIComponent(vhost)
    req.open('DELETE', finalUrl, false)
    req.setRequestHeader("Authorization", "Basic " + base64Credentials)
    
    req.send()
    if (req.status == 200 || req.status == 204) {
        log("Succesfully deleted vhost " + vhost)
        return 
    }else {
      error("status:" + req.status + " : " + req.responseText)
      throw new Error(req.responseText)
    }
  }


}
