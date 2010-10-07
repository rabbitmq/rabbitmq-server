import sys

#
# Functions for backwards compatibility
#

def get_func_argcount(func):
    """
    Return the argument count for a function
    """
    if sys.hexversion > 0x03000000:
        return func.__code__.co_argcount
    else:
        return func.func_code.co_argcount
        
def input_prompt(prompt):
    """
    Get user input
    """
    if sys.hexversion > 0x03000000:
        return input(prompt)
    else:
        return raw_input(prompt)
        
def join(chars):
    if sys.hexversion > 0x03000000:
        return bytes('', 'UTF-8').join(chars).decode('UTF-8')
    else:
        return ''.join(chars)

def socksend(conn, msg):
    if sys.hexversion > 0x03000000:
        conn.send(msg.encode())
    else:
        conn.send(msg)
        
        
def getheader(headers, key):
    if sys.hexversion > 0x03000000:
        return headers[key]
    else:
        return headers.getheader(key)