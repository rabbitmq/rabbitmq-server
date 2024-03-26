const XMLHttpRequest = require('xmlhttprequest').XMLHttpRequest

const baseURL = process.env.AUTH_BACKEND_HTTP_BASEURL || 'http://localhost:8888'

function putReset() {
  const req = new XMLHttpRequest()
  const url = baseURL + '/mockserver/reset'
  req.open('PUT', url, false)
  req.send()
  if (!wasSuccessful(req)) {
    console.error(req.responseText)
    throw new Error(req.responseText)
  }
}
function putExpectation(expectation) {
  const req = new XMLHttpRequest()
  const url = baseURL + '/mockserver/expectation'
  req.open('PUT', url, false)
  req.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded')
  req.setRequestHeader('Accept', 'application/json')
  req.send(JSON.stringify(expectation))
  if (!wasSuccessful(req)) {
    console.error(req.responseText)
    throw new Error(req.responseText)
  }
}
function wasSuccessful(req) {
  return Math.floor(req.status / 100) == 2
}
function putVerify(expectation) {
  const req = new XMLHttpRequest()
  const url = baseURL + '/mockserver/verify'

  req.open('PUT', url, false)
  req.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded')
  req.setRequestHeader('Accept', 'application/json')

  req.send(JSON.stringify(expectation))
  if (!wasSuccessful(req)) {
    console.error(req.responseText)
    throw new Error(req.responseText)
  }

}

module.exports = {

  deny: () => {
    return "deny"
  },
  allow: () => {
    return "allow"
  },

  reset: () => {
    putReset()
  },

  // let parameters = { username: 'something', password: 'somethingelse', param3: 'another' }
  // let response = "deny"
  // let allow = "allow"
  // let allow = "allow [administrator]"
  expectUser: (parameters, response, opts = { method: 'GET', path: '/auth/user'}) => {

    putExpectation({
        "httpRequest": {
          "method" : opts.method,
          "path": opts.path,
          "queryStringParameters": parameters
        },
        "httpResponse": {
          "body": response
        }
    })
    return {
        "httpRequest": {
          "method" : opts.method,
          "path": opts.path,
          "queryStringParameters": parameters
        },
        "times": {
          "atLeast": 1
        }
    }
  },
  expectVhost: (parameters, response, opts = { method: 'GET', path: '/auth/vhost'}) => {
    putExpectation({
        "httpRequest": {
          "method" : opts.method,
          "path": opts.path,
          "queryStringParameters": parameters
        },
        "httpResponse": {
          "body": response
        }
    })
    return {
        "httpRequest": {
          "method" : opts.method,
          "path": opts.path,
          "queryStringParameters": parameters
        },
        "times": {
          "atLeast": 1
        }
    }
  },

  expectResource: (parameters, response, opts = { method: 'GET', path: '/auth/resource'}) => {
    putExpectation({
        "httpRequest": {
          "method" : opts.method,
          "path": opts.path,
          "queryStringParameters": parameters
        },
        "httpResponse": {
          "body": response
        }
    })
    return {
        "httpRequest": {
          "method" : opts.method,
          "path": opts.path,
          "queryStringParameters": parameters
        },
        "times": {
          "atLeast": 1
        }
    }
  },
  verify: (expectation) => {
    putVerify(expectation)
  },
  verifyAll : (expectations) => {
    for (i in expectations) {
        putVerify(expectations[i])
    }
  }

}
