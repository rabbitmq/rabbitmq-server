from django.http import HttpResponse
from django.contrib.auth import authenticate

def user(request):
    if 'username' in request.GET and 'password' in request.GET:
        username = request.GET['username']
        password = request.GET['password']
        user = authenticate(username=username, password=password)
        if user:
            if user.is_superuser:
                return HttpResponse("allow administrator")
            else:
                return HttpResponse("allow management")
    return HttpResponse("deny")

def vhost(request):
    return HttpResponse("allow")

def resource(request):
    return HttpResponse("allow")
