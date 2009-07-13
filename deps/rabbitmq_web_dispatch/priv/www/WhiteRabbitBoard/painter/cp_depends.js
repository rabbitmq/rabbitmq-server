/*
	Base, version 1.0.1
	Copyright 2006, Dean Edwards
	License: http://creativecommons.org/licenses/LGPL/2.1/
*/
function Base() {
};
Base.version = "1.0.1";
Base.prototype = {
	extend: function(source, value) {
		var extend = Base.prototype.extend;
		if (arguments.length == 2) {
			var ancestor = this[source];
			// overriding?
			if ((ancestor instanceof Function) && (value instanceof Function) &&
				ancestor.valueOf() != value.valueOf() && /\binherit\b/.test(value)) {
				var method = value;
				value = function() {
					var previous = this.inherit;
					this.inherit = ancestor;
					var returnValue = method.apply(this, arguments);
					this.inherit = previous;
					return returnValue;
				};
				// point to the underlying method
				value.valueOf = function() {
					return method;
				};
				value.toString = function() {
					return String(method);
				};
			}
			return this[source] = value;
		} else if (source) {
			var _prototype = {toSource: null};
			// do the "toString" and other methods manually
			var _protected = ["toString", "valueOf"];
			// if we are prototyping then include the constructor
			if (Base._prototyping) _protected[2] = "constructor";
			for (var i = 0; (name = _protected[i]); i++) {
				if (source[name] != _prototype[name]) {
					extend.call(this, name, source[name]);
				}
			}
			// copy each of the source object's properties to this object
			for (var name in source) {
				if (!_prototype[name]) {
					extend.call(this, name, source[name]);
				}
			}
		}
		return this;
	},

	inherit: function() {
		// call this method from any other method to invoke that method's ancestor
	}
};

Base.extend = function(_instance, _static) {	
	var extend = Base.prototype.extend;
	if (!_instance) _instance = {};
	// create the constructor
	if (_instance.constructor == Object) {
		_instance.constructor = new Function;
	}
	// build the prototype
	Base._prototyping = true;
	var _prototype = new this;
	extend.call(_prototype, _instance);
	var constructor = _prototype.constructor;
	_prototype.constructor = this;
	delete Base._prototyping;
	// create the wrapper for the constructor function
	var klass = function() {
		if (!Base._prototyping) constructor.apply(this, arguments);
		this.constructor = klass;
	};
	klass.prototype = _prototype;
	// build the class interface
	klass.extend = this.extend;
	klass.toString = function() {
		return String(constructor);
	};
	extend.call(klass, _static);
	// support singletons
	var object = constructor ? klass : _prototype;
	// class initialisation
	if (object.init instanceof Function) object.init();
	return object;
};

/*  Prototype JavaScript framework, version 1.4.0
 *  (c) 2005 Sam Stephenson <sam@conio.net>
 *
 *  Prototype is freely distributable under the terms of an MIT-style license.
 *  For details, see the Prototype web site: http://prototype.conio.net/
 *
/*--------------------------------------------------------------------------*/
Function.prototype.bindAsEventListener = function(object) {
  var __method = this;
  return function(event) {
    return __method.call(object, event || window.event);
  }
}