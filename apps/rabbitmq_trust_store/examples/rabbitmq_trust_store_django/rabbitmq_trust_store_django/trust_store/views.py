import logging
import os

from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.conf import settings
from django.views.decorators.http import last_modified
from datetime import datetime

logger = logging.getLogger(__name__)

def latest_dir_change(request):
    timestamp = os.stat(cert_directory()).st_mtime
    dt = datetime.fromtimestamp(timestamp)
    logger.debug('latest_dir_change: %s', dt)
    return dt

@last_modified(latest_dir_change)
def index(request):
    request.META
    directory = cert_directory()
    certs = {'certificates': [file_object(file) for file in pem_files(directory)]}
    return JsonResponse(certs)

def cert_directory():
    return os.path.join(settings.BASE_DIR, "certs")

def pem_files(directory):
    files = os.listdir(directory)
    return [os.path.join(directory, file) for file in files if is_pem(file)]

def is_pem(file):
    return 'pem' == os.path.splitext(file)[1][1:]

def file_object(file):
    return {'id': file_id(file), 'path': path_for_file(file)}


def file_id(file):
    mtime = str(int(os.stat(file).st_mtime))
    basename = os.path.basename(file)
    return basename + ':' + mtime

def path_for_file(file):
    basename = os.path.basename(file)
    return "/certs/" + basename
