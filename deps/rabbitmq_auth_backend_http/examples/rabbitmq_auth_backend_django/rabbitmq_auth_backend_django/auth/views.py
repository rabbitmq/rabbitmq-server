from django.http import HttpResponse
from django.contrib.auth import authenticate
from django.views.decorators.csrf import csrf_exempt
import logging

logger = logging.getLogger('auth_backend')

@csrf_exempt
def user(request):
    logger.info(request.GET)
    if 'username' in request.GET and 'password' in request.GET:
        username = request.GET['username']
        password = request.GET['password']
        if username == 'admin':
            return HttpResponse("allow administrator")

        if username == 'someuser':
            return HttpResponse("allow")

        user = authenticate(username=username, password=password)
        if user:
            if user.is_superuser:
                return HttpResponse("allow administrator")
            else:
                return HttpResponse("allow management")
    return HttpResponse("deny")

@csrf_exempt
def vhost(request):
    logger.info(request.GET)
    return HttpResponse("allow")

@csrf_exempt
def resource(request):
    logger.info(request.GET)
    return HttpResponse("allow")

@csrf_exempt
def topic(request):
    logger.info(request.GET)
    return HttpResponse("allow")
