#!/usr/bin/env python2

import mailbox
import imaplib
import os.path
import datetime
import email.utils
import progressbar
import logging

# imaps://username@domain.com:password@imap.server:port/folder
# mbox:///path/to/file.mbox
# maildir:///path/to/maildir

class MailSyncer(object):
    DATE_HEADER_FORMAT      = '%a, %d %b %Y %H:%M:%S %z'
    INTERNALDATE_FORMAT     = '%d-%b-%Y %H:%M:%S +0000'

    def __init__(self, source_dsn, dest_dsn):
        self.log = self._create_logger()
        self.source_dsn, self.source = self._create_target(source_dsn)
        self.dest_dsn, self.dest = self._create_target(dest_dsn)

    def _convert_date_header_to_tuple(self, date_str):
        parsed_date = email.utils.parsedate_tz(date_str)
        timestamp = datetime.datetime(*parsed_date[:6]) - datetime.timedelta(seconds=parsed_date[-1])
        timetuple = timestamp.utctimetuple()
        # self.log.debug('Converted Date header: %s -> %s', date_str, str(timetuple))
        return timetuple

    def _create_logger(self):
        logger = logging.getLogger('{0}[{1}]'.format(self.__class__.__name__, id(self)))
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(logging.DEBUG)
        logger.propagate = True
        return logger

    def _parse_dsn(self, dsn_str):
        dsn = {
            'type':         None,
            'hostname':     None,
            'port':         None,
            'username':     None,
            'password':     None,
            'path':         None,
        }
        dsn_type, conn_str = dsn_str.split('://')
        dsn['type'] = dsn_type.lower()
        if dsn['type'].startswith('imap'):
            auth, svr_info = conn_str.rsplit('@', 1)
            dsn['username'], dsn['password'] = auth.split(':', 1)
            conn, path = svr_info.split('/', 1)
            dsn['hostname'], dsn['port'] = conn.split(':')
            dsn['port'] = int(dsn['port'])
            dsn['path'] = path
        else:
            dsn['type'] = dsn_type
            dsn['path'] = conn_str

        self.log.debug('Parsed "%s" into "%s"', dsn_str, str(dsn))

        return dsn

    def _create_target(self, dsn):
        parsed_dsn = self._parse_dsn(dsn)
        target = {
            'imap': lambda dsn: self._create_target_imap4(dsn, False),
            'imaps': lambda dsn: self._create_target_imap4(dsn, True),
            'mbox': self._create_target_mbox,
            'maildir': self._create_target_maildir,
        }.get(parsed_dsn['type'])(parsed_dsn)
        return parsed_dsn, target

    def _create_target_mbox(self, dsn):
        conn = mailbox.mbox(dsn['path'], create=False)
        self.log.debug('Created MBOX target for %s', dsn['path'])
        return conn

    def _create_target_maildir(self, dsn):
        conn = mailbox.Maildir(dsn['path'], factory=None, create=False)
        self.log.debug('Created Maildir target for %s', dsn['path'])
        return conn

    def _create_target_imap4(self, dsn, ssl=True):
        if ssl:
            imap_cls = imaplib.IMAP4_SSL
        else:
            imap_cls = imaplib.IMAP4

        conn = imap_cls(dsn['hostname'], dsn['port'])
        self.log.debug('Created IMAP target for %s:%d', dsn['hostname'], dsn['port'])
        if ssl:
            self.log.debug('Using IMAP over SSL')

        conn.login(dsn['username'], dsn['password'])
        self.log.debug('Logged in using %s:%s', dsn['username'], dsn['password'])

        conn.select(dsn['path'])
        self.log.debug('Selected folder: %s', dsn['path'])

        return conn

    def _iterate_messages(self, mailbox):
        msg_i = 1
        total_msgs = len(mailbox) + 1
        for message in mailbox:
            msg_date = self._convert_date_header_to_tuple(message.get('Date'))
            yield (msg_i, total_msgs, message)
            msg_i += 1

    def _copy_message(self, message, dest, mark_as_read=True):
        if mark_as_read:
            flags = r'(\Seen)'
        else:
            flags = None

        self.log.info('Copying message: %s -> %s @ %s', message.get('From'),
            message.get('Subject'), message.get('Date'))
        try:
            dest.append(self.dest_dsn['path'], flags, self._convert_date_header_to_tuple(message.get('Date')),
                message.as_string())
        except Exception as err:
            self.log.exception(err)


    def copy(self, progress_observer=None):
        self.log.info('Starting copy...')
        for i, total, message in self._iterate_messages(self.source):
            self._copy_message(message, self.dest)
        self.log.info('Copy finished')

if __name__ == '__main__':
    import sys

    copier = MailSyncer(sys.argv[1], sys.argv[2])
    copier.copy()
