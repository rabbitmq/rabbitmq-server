function Application(configFn) {
    this._routes = { get: [], put: [], post: [], del: [] };
    this.title_function = null;
    this._hashchange_handler = null;
    this._submit_handler = null;

    if (configFn) {
        configFn.call(this);
    }
}

Application.prototype._compile = function(path) {
    var paramNames = [];
    var pattern = path.replace(/:([^\/]+)/g, function(_, name) {
        paramNames.push(name);
        return '([^/]+)';
    });
    return { regex: new RegExp('^' + pattern + '$'), paramNames: paramNames };
};

Application.prototype._addRoute = function(verb, path, callback) {
    var compiled = this._compile(path);
    this._routes[verb].push({
        regex: compiled.regex,
        paramNames: compiled.paramNames,
        callback: callback
    });
};

Application.prototype.get = function(path, callback) {
    this._addRoute('get', path, callback);
};

Application.prototype.put = function(path, callback) {
    this._addRoute('put', path, callback);
};

Application.prototype.post = function(path, callback) {
    this._addRoute('post', path, callback);
};

Application.prototype.del = function(path, callback) {
    this._addRoute('del', path, callback);
};

Application.prototype.setTitle = function(title) {
    if (typeof title === 'function') {
        this.title_function = title;
    } else {
        this.title_function = function(additional) {
            return title + ' ' + additional;
        };
    }
};

Application.prototype.use = function(pluginName) {
};

Application.prototype.helper = function(name, fn) {
};

Application.prototype._runRoute = function(verb, path, extraParams) {
    var routes = this._routes[verb];
    for (var i = 0; i < routes.length; i++) {
        var route = routes[i];
        var match = route.regex.exec(path);
        if (match) {
            var params = {};
            for (var j = 0; j < route.paramNames.length; j++) {
                params[route.paramNames[j]] = match[j + 1];
            }
            if (extraParams) {
                for (var key in extraParams) {
                    if (extraParams.hasOwnProperty(key)) {
                        params[key] = extraParams[key];
                    }
                }
            }
            var context = new RouteContext(this, params);
            return route.callback.call(context);
        }
    }
};

Application.prototype.run = function() {
    var app = this;

    this._hashchange_handler = function() {
        var hash = window.location.hash || '#/';
        app._runRoute('get', hash);
    };
    window.addEventListener('hashchange', this._hashchange_handler);

    this._submit_handler = function(e) {
        var form = e.target;
        var verb = (form.getAttribute('method') || 'get').toLowerCase();
        var path = form.getAttribute('action') || '';

        if (verb === 'get') {
            var qs = $(form).serialize();
            e.preventDefault();
            window.location.hash = path + '?' + qs;
            return;
        }

        if (verb === 'delete') {
            verb = 'del';
        }

        var params = {};
        var fields = $(form).serializeArray();
        for (var i = 0; i < fields.length; i++) {
            var field = fields[i];
            if (params.hasOwnProperty(field.name)) {
                if (!Array.isArray(params[field.name])) {
                    params[field.name] = [params[field.name]];
                }
                params[field.name].push(field.value);
            } else {
                params[field.name] = field.value;
            }
        }

        e.preventDefault();
        app._runRoute(verb, path, params);
    };
    $(document).on('submit', 'form', this._submit_handler);

    var hash = window.location.hash || '#/';
    this._runRoute('get', hash);
};

Application.prototype.unload = function() {
    if (this._hashchange_handler) {
        window.removeEventListener('hashchange', this._hashchange_handler);
        this._hashchange_handler = null;
    }
    if (this._submit_handler) {
        $(document).off('submit', 'form', this._submit_handler);
        this._submit_handler = null;
    }
    this._routes = { get: [], put: [], post: [], del: [] };
};

function RouteContext(app, params) {
    this.app = app;
    this.params = params;
}

RouteContext.prototype.title = function() {
    var new_title = Array.prototype.slice.call(arguments).join(' ');
    if (this.app.title_function) {
        new_title = this.app.title_function(new_title);
    }
    document.title = new_title;
    return new_title;
};

var Sammy = { Application: Application };
