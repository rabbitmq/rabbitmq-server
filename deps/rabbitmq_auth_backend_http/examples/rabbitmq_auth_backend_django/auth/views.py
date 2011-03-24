from django.http import HttpResponse
from django.contrib.auth import authenticate

def user(request):
    if 'username' in request.GET and 'password' in request.GET:
        username = request.GET['username']
        password = request.GET['password']
        if authenticate(username=username, password=password):
            return HttpResponse("true")
    return HttpResponse("false")

def vhost(request):
    return HttpResponse("true")

def resource(request):
    return HttpResponse("true")
