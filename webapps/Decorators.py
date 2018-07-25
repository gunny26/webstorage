#!/usr/bin/python
""" some common used decorators """
import time
import logging
import web

def authenticator(config):
    def real_authenticator(func):
        """
        decorator for authentication
        """
        log = logging.getLogger(func.__name__)
        def inner(*args, **kwds):
            call_str = "%s(%s, %s)" % (func.__name__, args[1:], kwds)
            log.debug("calling %s", call_str)
            try:
                if web.ctx.env.get("HTTP_X_AUTH_TOKEN") is not None:
                    if web.ctx.env.get("HTTP_X_AUTH_TOKEN") not in config["apikeys"]:
                        log.error("X-AUTH-TOKEN %s not in allowed APIKEYS", web.ctx.env.get("HTTP_X_AUTH_TOKEN"))
                        web.ctx.status = '401 Unauthorized'
                    else:
                        # authorization OK
                        log.debug("successfully autorized with api-key %s", web.ctx.env.get("HTTP_X_AUTH_TOKEN")) 
                        ret_val = func(*args, **kwds)
                        return ret_val
                else:
                    log.error("X-AUTH-TOKEN HTTP Header missing")
                    web.ctx.status = '401 Unauthorized'
                return
            except Exception as exc:
                log.exception(exc)
                log.error("call to %s caused Exception", call_str)
                web.internalerror()
        # set inner function __name__ and __doc__ to original ones
        inner.__name__ = func.__name__
        inner.__doc__ = func.__doc__
        return inner
    return real_authenticator

def calllogger(func):
    """
    decorator for logging call arguments and duration
    """
    log = logging.getLogger(func.__name__)
    def inner(*args, **kwds):
        starttime = time.time()
        call_str = "%s(%s, %s)" % (func.__name__, args[1:], kwds)
        log.debug("calling %s", call_str)
        try:
            ret_val = func(*args, **kwds)
            log.debug("duration of call %s : %s", call_str, (time.time() - starttime))
            return ret_val
        except Exception as exc:
            log.exception(exc)
            log.error("call to %s caused Exception", call_str)
            web.internalerror()
    # set inner function __name__ and __doc__ to original ones
    inner.__name__ = func.__name__
    inner.__doc__ = func.__doc__
    return inner



