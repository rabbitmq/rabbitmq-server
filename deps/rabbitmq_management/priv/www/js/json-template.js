// Copyright (C) 2009 Andy Chu
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// $Id$

//
// JavaScript implementation of json-template.
//

// This is predefined in tests, shouldn't be defined anywhere else.  TODO: Do
// something nicer.
var log = log || function() {};
var repr = repr || function() {};


// The "module" exported by this script is called "jsontemplate":

var jsontemplate = function() {


// Regex escaping for metacharacters
function EscapeMeta(meta) {
  return meta.replace(/([\{\}\(\)\[\]\|\^\$\-\+\?])/g, '\\$1');
}

var token_re_cache = {};

function _MakeTokenRegex(meta_left, meta_right) {
  var key = meta_left + meta_right;
  var regex = token_re_cache[key];
  if (regex === undefined) {
    var str = '(' + EscapeMeta(meta_left) + '.*?' + EscapeMeta(meta_right) +
              '\n?)';
    regex = new RegExp(str, 'g');
  }
  return regex;
}

//
// Formatters
//

function HtmlEscape(s) {
  return s.replace(/&/g,'&amp;').
           replace(/>/g,'&gt;').
           replace(/</g,'&lt;');
}

function HtmlTagEscape(s) {
  return s.replace(/&/g,'&amp;').
           replace(/>/g,'&gt;').
           replace(/</g,'&lt;').
           replace(/"/g,'&quot;');
}

// Default ToString can be changed
function ToString(s) {
  if (s === null) {
    return 'null';
  }
  return s.toString();
}

// Formatter to pluralize words
function _Pluralize(value, unused_context, args) {
  var s, p;
  switch (args.length) {
    case 0:
      s = ''; p = 's';
      break;
    case 1:
      s = ''; p = args[0];
      break;
    case 2:
      s = args[0]; p = args[1];
      break;
    default:
      // Should have been checked at compile time
      throw {
        name: 'EvaluationError', message: 'pluralize got too many args'
      };
  }
  return (value > 1) ? p : s;
}

function _Cycle(value, unused_context, args) {
  // Cycle between various values on consecutive integers.
  // @index starts from 1, so use 1-based indexing.
  return args[(value - 1) % args.length];
}

var DEFAULT_FORMATTERS = {
  'html': HtmlEscape,
  'htmltag': HtmlTagEscape,
  'html-attr-value': HtmlTagEscape,
  'str': ToString,
  'raw': function(x) { return x; },
  'AbsUrl': function(value, context) {
    // TODO: Normalize leading/trailing slashes
    return context.get('base-url') + '/' + value;
  }
};

var DEFAULT_PREDICATES = {
  'singular?': function(x) { return  x == 1; },
  'plural?': function(x) { return x > 1; },
  'Debug?': function(unused, context) {
    try {
      return context.get('debug');
    } catch(err) {
      if (err.name == 'UndefinedVariable') {
        return false;
      } else {
        throw err;
      }
    }
  }
};

var FunctionRegistry = function() {
  return {
    lookup: function(user_str) {
      return [null, null];
    }
  };
};

var SimpleRegistry = function(obj) {
  return {
    lookup: function(user_str) {
      var func = obj[user_str] || null;
      return [func, null];
    }
  };
};

var CallableRegistry = function(callable) {
  return {
    lookup: function(user_str) {
      var func = callable(user_str);
      return [func, null];
    }
  };
};

// Default formatters which can't be expressed in DEFAULT_FORMATTERS
var PrefixRegistry = function(functions) {
  return {
    lookup: function(user_str) {
      for (var i = 0; i < functions.length; i++) {
        var name = functions[i].name, func = functions[i].func;
        if (user_str.slice(0, name.length) == name) {
          // Delimiter is usually a space, but could be something else
          var args;
          var splitchar = user_str.charAt(name.length);
          if (splitchar === '') {
            args = [];  // No arguments
          } else {
            args = user_str.split(splitchar).slice(1);
          }
          return [func, args];
        } 
      }
      return [null, null];  // No formatter
    }
  };
};

var ChainedRegistry = function(registries) {
  return {
    lookup: function(user_str) {
      for (var i=0; i<registries.length; i++) {
        var result = registries[i].lookup(user_str);
        if (result[0]) {
          return result;
        }
      }
      return [null, null];  // Nothing found
    }
  };
};

//
// Template implementation
//

function _ScopedContext(context, undefined_str) {
  // The stack contains:
  //   The current context (an object).
  //   An iteration index.  -1 means we're NOT iterating.
  var stack = [{context: context, index: -1}];

  return {
    PushSection: function(name) {
      if (name === undefined || name === null) {
        return null;
      }
      var new_context;
      if (name == '@') {
        new_context = stack[stack.length-1].context;
      } else {
        new_context = stack[stack.length-1].context[name] || null;
      }
      stack.push({context: new_context, index: -1});
      return new_context;
    },

    Pop: function() {
      stack.pop();
    },

    next: function() {
      var stacktop = stack[stack.length-1];

      // Now we're iterating -- push a new mutable object onto the stack
      if (stacktop.index == -1) {
        stacktop = {context: null, index: 0};
        stack.push(stacktop);
      }

      // The thing we're iterating over
      var context_array = stack[stack.length-2].context;

      // We're already done
      if (stacktop.index == context_array.length) {
        stack.pop();
        return undefined;  // sentinel to say that we're done
      }

      stacktop.context = context_array[stacktop.index++];
      return true;  // OK, we mutated the stack
    },

    _Undefined: function(name) {
      if (undefined_str === undefined) {
        throw {
          name: 'UndefinedVariable', message: name + ' is not defined'
        };
      } else {
        return undefined_str;
      }
    },

    _LookUpStack: function(name) {
      var i = stack.length - 1;
      while (true) {
        var frame = stack[i];
        if (name == '@index') {
          if (frame.index != -1) {  // -1 is undefined
            return frame.index;
          }
        } else {
          var context = frame.context;
          if (typeof context === 'object') {
            var value = context[name];
            if (value !== undefined) {
              return value;
            }
          }
        }
        i--;
        if (i <= -1) {
          return this._Undefined(name);
        }
      }
    },

    get: function(name) {
      if (name == '@') {
        return stack[stack.length-1].context;
      }
      var parts = name.split('.');
      var value = this._LookUpStack(parts[0]);
      if (parts.length > 1) {
        for (var i=1; i<parts.length; i++) {
          value = value[parts[i]];
          if (value === undefined) {
            return this._Undefined(parts[i]);
          }
        }
      }
      return value;
    }

  };
}


// Crockford's "functional inheritance" pattern

var _AbstractSection = function(spec) {
  var that = {};
  that.current_clause = [];

  that.Append = function(statement) {
    that.current_clause.push(statement);
  };

  that.AlternatesWith = function() {
    throw {
      name: 'TemplateSyntaxError',
      message:
          '{.alternates with} can only appear with in {.repeated section ...}'
    };
  };

  that.NewOrClause = function(pred) {
    throw { name: 'NotImplemented' };  // "Abstract"
  };

  return that;
};

var _Section = function(spec) {
  var that = _AbstractSection(spec);
  that.statements = {'default': that.current_clause};

  that.section_name = spec.section_name;

  that.Statements = function(clause) {
    clause = clause || 'default';
    return that.statements[clause] || [];
  };

  that.NewOrClause = function(pred) {
    if (pred) {
      throw {
        name: 'TemplateSyntaxError',
        message: '{.or} clause only takes a predicate inside predicate blocks'
      };
    }
    that.current_clause = [];
    that.statements['or'] = that.current_clause;
  };

  return that;
};

// Repeated section is like section, but it supports {.alternates with}
var _RepeatedSection = function(spec) {
  var that = _Section(spec);

  that.AlternatesWith = function() {
    that.current_clause = [];
    that.statements['alternate'] = that.current_clause;
  };

  return that;
};

// Represents a sequence of predicate clauses.
var _PredicateSection = function(spec) {
  var that = _AbstractSection(spec);
  // Array of func, statements
  that.clauses = [];

  that.NewOrClause = function(pred) {
    // {.or} always executes if reached, so use identity func with no args
    pred = pred || [function(x) { return true; }, null];
    that.current_clause = [];
    that.clauses.push([pred, that.current_clause]);
  };

  return that;
};


function _Execute(statements, context, callback) {
  for (var i=0; i<statements.length; i++) {
    var statement = statements[i];

    if (typeof(statement) == 'string') {
      callback(statement);
    } else {
      var func = statement[0];
      var args = statement[1];
      func(args, context, callback);
    }
  }
}

function _DoSubstitute(statement, context, callback) {
  var value;
  value = context.get(statement.name);

  // Format values
  for (var i=0; i<statement.formatters.length; i++) {
    var pair = statement.formatters[i];
    var formatter = pair[0];
    var args = pair[1];
    value = formatter(value, context, args);
  }

  callback(value);
}

// for [section foo]
function _DoSection(args, context, callback) {

  var block = args;
  var value = context.PushSection(block.section_name);
  var do_section = false;

  // "truthy" values should have their sections executed.
  if (value) {
    do_section = true;
  }
  // Except: if the value is a zero-length array (which is "truthy")
  if (value && value.length === 0) {
    do_section = false;
  }

  if (do_section) {
    _Execute(block.Statements(), context, callback);
    context.Pop();
  } else {  // Empty list, None, False, etc.
    context.Pop();
    _Execute(block.Statements('or'), context, callback);
  }
}

// {.pred1?} A {.or pred2?} B ... {.or} Z {.end}
function _DoPredicates(args, context, callback) {
  // Here we execute the first clause that evaluates to true, and then stop.
  var block = args;
  var value = context.get('@');
  for (var i=0; i<block.clauses.length; i++) {
    var clause = block.clauses[i];
    var predicate = clause[0][0];
    var pred_args = clause[0][1];
    var statements = clause[1];

    var do_clause = predicate(value, context, pred_args);
    if (do_clause) {
      _Execute(statements, context, callback);
      break;
    }
  }
}


function _DoRepeatedSection(args, context, callback) {
  var block = args;

  items = context.PushSection(block.section_name);
  pushed = true;

  if (items && items.length > 0) {
    // TODO: check that items is an array; apparently this is hard in JavaScript
    //if type(items) is not list:
    //  raise EvaluationError('Expected a list; got %s' % type(items))

    // Execute the statements in the block for every item in the list.
    // Execute the alternate block on every iteration except the last.  Each
    // item could be an atom (string, integer, etc.) or a dictionary.
    
    var last_index = items.length - 1;
    var statements = block.Statements();
    var alt_statements = block.Statements('alternate');

    for (var i=0; context.next() !== undefined; i++) {
      _Execute(statements, context, callback);
      if (i != last_index) {
        _Execute(alt_statements, context, callback);
      }
    }
  } else {
    _Execute(block.Statements('or'), context, callback);
  }

  context.Pop();
}


var _SECTION_RE = /(repeated)?\s*(section)\s+(\S+)?/;
var _OR_RE = /or(?:\s+(.+))?/;
var _IF_RE = /if(?:\s+(.+))?/;


// Turn a object literal, function, or Registry into a Registry
function MakeRegistry(obj) {
  if (!obj) {
    // if null/undefined, use a totally empty FunctionRegistry
    return new FunctionRegistry();
  } else if (typeof obj === 'function') {
    return new CallableRegistry(obj);
  } else if (obj.lookup !== undefined) {
    // TODO: Is this a good pattern?  There is a namespace conflict where get
    // could be either a formatter or a method on a FunctionRegistry.
    // instanceof might be more robust.
    return obj;
  } else if (typeof obj === 'object') {
    return new SimpleRegistry(obj);
  }
}

// TODO: The compile function could be in a different module, in case we want to
// compile on the server side.
function _Compile(template_str, options) {
  var more_formatters = MakeRegistry(options.more_formatters);

  // default formatters with arguments
  var default_formatters = PrefixRegistry([
      {name: 'pluralize', func: _Pluralize},
      {name: 'cycle', func: _Cycle}
      ]);
  var all_formatters = new ChainedRegistry([
      more_formatters,
      SimpleRegistry(DEFAULT_FORMATTERS),
      default_formatters
      ]);

  var more_predicates = MakeRegistry(options.more_predicates);

  // TODO: Add defaults
  var all_predicates = new ChainedRegistry([
      more_predicates, SimpleRegistry(DEFAULT_PREDICATES)
      ]);

  // We want to allow an explicit null value for default_formatter, which means
  // that an error is raised if no formatter is specified.
  var default_formatter;
  if (options.default_formatter === undefined) {
    default_formatter = 'str';
  } else {
    default_formatter = options.default_formatter;
  }

  function GetFormatter(format_str) {
    var pair = all_formatters.lookup(format_str);
    if (!pair[0]) {
      throw {
        name: 'BadFormatter',
        message: format_str + ' is not a valid formatter'
      };
    }
    return pair;
  }

  function GetPredicate(pred_str) {
    var pair = all_predicates.lookup(pred_str);
    if (!pair[0]) {
      throw {
        name: 'BadPredicate',
        message: pred_str + ' is not a valid predicate'
      };
    }
    return pair;
  }

  var format_char = options.format_char || '|';
  if (format_char != ':' && format_char != '|') {
    throw {
      name: 'ConfigurationError',
      message: 'Only format characters : and | are accepted'
    };
  }

  var meta = options.meta || '{}';
  var n = meta.length;
  if (n % 2 == 1) {
    throw {
      name: 'ConfigurationError',
      message: meta + ' has an odd number of metacharacters'
    };
  }
  var meta_left = meta.substring(0, n/2);
  var meta_right = meta.substring(n/2, n);

  var token_re = _MakeTokenRegex(meta_left, meta_right);
  var current_block = _Section({});
  var stack = [current_block];

  var strip_num = meta_left.length;  // assume they're the same length

  var token_match;
  var last_index = 0;

  while (true) {
    token_match = token_re.exec(template_str);
    if (token_match === null) {
      break;
    } else {
      var token = token_match[0];
    }

    // Add the previous literal to the program
    if (token_match.index > last_index) {
      var tok = template_str.slice(last_index, token_match.index);
      current_block.Append(tok);
    }
    last_index = token_re.lastIndex;

    var had_newline = false;
    if (token.slice(-1) == '\n') {
      token = token.slice(null, -1);
      had_newline = true;
    }

    token = token.slice(strip_num, -strip_num);

    if (token.charAt(0) == '#') {
      continue;  // comment
    }

    if (token.charAt(0) == '.') {  // Keyword
      token = token.substring(1, token.length);

      var literal = {
          'meta-left': meta_left,
          'meta-right': meta_right,
          'space': ' ',
          'tab': '\t',
          'newline': '\n'
          }[token];

      if (literal !== undefined) {
        current_block.Append(literal);
        continue;
      }

      var new_block, func;

      var section_match = token.match(_SECTION_RE);
      if (section_match) {
        var repeated = section_match[1];
        var section_name = section_match[3];

        if (repeated) {
          func = _DoRepeatedSection;
          new_block = _RepeatedSection({section_name: section_name});
        } else {
          func = _DoSection;
          new_block = _Section({section_name: section_name});
        }
        current_block.Append([func, new_block]);
        stack.push(new_block);
        current_block = new_block;
        continue;
      }

      var pred_str, pred;

      // Check {.or pred?} before {.pred?}
      var or_match = token.match(_OR_RE);
      if (or_match) {
        pred_str = or_match[1];
        pred = pred_str ? GetPredicate(pred_str) : null;
        current_block.NewOrClause(pred);
        continue;
      }

      // Match either {.pred?} or {.if pred?}
      var matched = false;

      var if_match = token.match(_IF_RE);
      if (if_match) {
        pred_str = if_match[1];
        matched = true;
      } else if (token.charAt(token.length-1) == '?') {
        pred_str = token;
        matched = true;
      }
      if (matched) {
        pred = pred_str ? GetPredicate(pred_str) : null;
        new_block = _PredicateSection();
        new_block.NewOrClause(pred);
        current_block.Append([_DoPredicates, new_block]);
        stack.push(new_block);
        current_block = new_block;
        continue;
      }

      if (token == 'alternates with') {
        current_block.AlternatesWith();
        continue;
      }

      if (token == 'end') {
        // End the block
        stack.pop();
        if (stack.length > 0) {
          current_block = stack[stack.length-1];
        } else {
          throw {
            name: 'TemplateSyntaxError',
            message: 'Got too many {end} statements'
          };
        }
        continue;
      }
    }

    // A variable substitution
    var parts = token.split(format_char);
    var formatters;
    var name;
    if (parts.length == 1) {
      if (default_formatter === null) {
          throw {
            name: 'MissingFormatter',
            message: 'This template requires explicit formatters.'
          };
      }
      // If no formatter is specified, use the default.
      formatters = [GetFormatter(default_formatter)];
      name = token;
    } else {
      formatters = [];
      for (var j=1; j<parts.length; j++) {
        formatters.push(GetFormatter(parts[j]));
      }
      name = parts[0];
    }
    current_block.Append([_DoSubstitute, {name: name, formatters: formatters}]);
    if (had_newline) {
      current_block.Append('\n');
    }
  }

  // Add the trailing literal
  current_block.Append(template_str.slice(last_index));

  if (stack.length !== 1) {
    throw {
      name: 'TemplateSyntaxError',
      message: 'Got too few {end} statements'
    };
  }
  return current_block;
}

// The Template class is defined in the traditional style so that users can add
// methods by mutating the prototype attribute.  TODO: Need a good idiom for
// inheritance without mutating globals.

function Template(template_str, options) {

  // Add 'new' if we were not called with 'new', so prototyping works.
  if(!(this instanceof Template)) {
    return new Template(template_str, options);
  }

  this._options = options || {};
  this._program = _Compile(template_str, this._options);
}

Template.prototype.render = function(data_dict, callback) {
  // options.undefined_str can either be a string or undefined
  var context = _ScopedContext(data_dict, this._options.undefined_str);
  _Execute(this._program.Statements(), context, callback);
};

Template.prototype.expand = function(data_dict) {
  var tokens = [];
  this.render(data_dict, function(x) { tokens.push(x); });
  return tokens.join('');
};

// fromString is a construction method that allows metadata to be written at the
// beginning of the template string.  See Python's FromFile for a detailed
// description of the format.
//
// The argument 'options' takes precedence over the options in the template, and
// can be used for non-serializable options like template formatters.

var OPTION_RE = /^([a-zA-Z\-]+):\s*(.*)/;
var OPTION_NAMES = [
    'meta', 'format-char', 'default-formatter', 'undefined-str'];
// Use this "linear search" instead of Array.indexOf, which is nonstandard
var OPTION_NAMES_RE = new RegExp(OPTION_NAMES.join('|'));

function fromString(s, options) {
  var parsed = {};
  var begin = 0, end = 0;

  while (true) {
    var parsedOption = false;
    end = s.indexOf('\n', begin);
    if (end == -1) {
      break;
    }
    var line = s.slice(begin, end);
    begin = end+1;
    var match = line.match(OPTION_RE);
    if (match !== null) {
      var name = match[1].toLowerCase(), value = match[2];
      if (name.match(OPTION_NAMES_RE)) {
        name = name.replace('-', '_');
        value = value.replace(/^\s+/, '').replace(/\s+$/, '');
        if (name == 'default_formatter' && value.toLowerCase() == 'none') {
          value = null;
        }
        parsed[name] = value;
        parsedOption = true;
      }
    }
    if (!parsedOption) {
      break;
    }
  }
  // TODO: This doesn't enforce the blank line between options and template, but
  // that might be more trouble than it's worth
  if (parsed !== {}) {
    body = s.slice(begin);
  } else {
    body = s;
  }
  for (var o in options) {
    parsed[o] = options[o];
  }
  return Template(body, parsed);
}


// We just export one name for now, the Template "class".
// We need HtmlEscape in the browser tests, so might as well export it.

return {
    Template: Template, HtmlEscape: HtmlEscape,
    FunctionRegistry: FunctionRegistry, SimpleRegistry: SimpleRegistry,
    CallableRegistry: CallableRegistry, ChainedRegistry: ChainedRegistry,
    fromString: fromString,
    // Private but exposed for testing
    _Section: _Section
    };

}();
