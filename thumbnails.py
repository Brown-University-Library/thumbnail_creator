import logging
import logging.handlers
from os import environ
from eulfedora.server import Repository
from rdflib import Namespace
import requests
from bdrcmodels.models import MasterImage


def get_env_setting(setting):
    """ Get the environment setting or return exception """
    try:
        return environ[setting]
    except KeyError:
        error_msg = "Set the %s env variable" % setting
        raise Exception(error_msg)

def setup_logger(filename):
    '''Configures a logger to write to console & <filename>.  '''
    formatter = logging.Formatter(u'%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(u'logger')
    logger.setLevel(logging.DEBUG)
    file_handler = logging.handlers.RotatingFileHandler(filename, maxBytes=5000000, backupCount=5)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter(u'%(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    return logger

class ThumbnailCreator(object):
    def __init__(self, logger, repo, thumbnail_server):
        self.logger = logger
        self.repo = repo
        self.thumbnail_server = thumbnail_server

    def create_thumbnail(self, pid, force=False):
        '''Use force=True to create a thumbnail even if there already is one.'''
        self.logger.info('%s: creating thumbnail' % pid)
        #get object & see if there's a thumbnail
        obj = self.repo.get_object(pid=pid, type=MasterImage)
        datastreams = obj.ds_list
        if force == False and self._has_thumbnail(datastreams):
            self.logger.info('%s: thumbnail datastream already exists.' % pid)
        else:
            thumbnail_url = self._get_thumbnail_url(pid)
            if thumbnail_url:
                obj.thumbnail.ds_location = thumbnail_url
                obj.thumbnail.label = 'thumbnail'
                try:
                    obj.save()
                    self.logger.info('%s: thumbnail saved.' % pid)
                except Exception as e:
                    self.logger.error('%s: exception saving changes: %s' % (pid, repr(e)))

    def _get_thumbnail_url(self, pid):
        url = self._build_thumbnail_svc_uri(pid)
        resp = requests.get(url)
        if resp.ok:
            #see if there's a history - if we got a redirect, don't return the content
            if resp.history:
                self.logger.warning('%s: got a redirect from thumbnail svc - new url: %s' % (pid, resp.url))
            else:
                return url
        else:
            self.logger.error('%s: error from thumbnail svc - url %s' % (pid, djatoka_url))
            self.logger.error('%s: thumbnail response: %s %s' % (pid, resp.status_code, resp.text))

    def _build_thumbnail_svc_uri(self, pid):
        return 'https://%s/viewers/image/thumbnail/%s/' % (self.thumbnail_server, pid)

    def _has_thumbnail(self, datastreams):
        THUMBNAIL_DATASTREAMS = ['thumbnail', 'THUMBNAIL', 'Thumbnail']
        if set(datastreams).isdisjoint(THUMBNAIL_DATASTREAMS):
            #no elements of datastreams are also in THUMBNAIL_DATASTREAMS,
            # so there's no thumbnail
            return False
        else:
            return True


logger = setup_logger('logs/thumbnail_creator.log')
fedora_root = 'https://%s/fedora/' % get_env_setting('FEDORA_SERVER')
fedora_user = get_env_setting('FEDORA_USER')
fedora_pass = get_env_setting('FEDORA_PASS')
repo = Repository(root=fedora_root, username=fedora_user, password=fedora_pass)
thumbnail_host = get_env_setting('FEDORA_SERVER')
tc = ThumbnailCreator(logger, repo, thumbnail_host)


def create_thumbnail(pid, force=False):
    tc.create_thumbnail(pid, force=force)

