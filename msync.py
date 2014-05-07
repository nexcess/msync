#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# Copyright (C) 2014 Nexcess.net LLC
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

"""
Msync - A mailbox copier with support for IMAP4, mbox, and Maildir (qmail)
mailboxes.


"""

import mailbox
import imaplib
import os.path
import datetime
import email.utils
import logging
import threading
import Queue
import time

def convert_date_header_to_tuple(date_str):
    """Convert a date from $DATE_HEADER_FORMAT to a timestruct tuple that the
    imap APPEND command will accept
    """

    # this may also work
    # imaplib.Time2Internaldate(email.utils.parsedate_tz(date_str)[:-1])
    parsed_date = email.utils.parsedate_tz(date_str)
    timestamp = datetime.datetime(*parsed_date[:6])
    if parsed_date[-1]:
         timestamp = timestamp - datetime.timedelta(seconds=parsed_date[-1])
    return timetuple

class MailSyncer(object):
    DATE_HEADER_FORMAT      = '%a, %d %b %Y %H:%M:%S %z'
    INTERNALDATE_FORMAT     = '%d-%b-%Y %H:%M:%S +0000'
    MAX_QUEUE_SIZE          = 4096

    def __init__(self, source_dsn, dest_dsn):
        self.log = self._create_logger()
        self.source_dsn, self.source = self._create_target(source_dsn)
        self.dest_dsn, self.dest = self._create_target(dest_dsn)

    def _create_logger(self):
        """Create a logger instance using this object's class name and the object
        ID (in case of multithreading weirdness)
        """

        logger = logging.getLogger('{0}[{1}]'.format(self.__class__.__name__, id(self)))
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(logging.INFO)
        logger.propagate = True
        return logger

    def _parse_dsn(self, dsn_str):
        """Parse a DSN string into a dictionary of its parts

        DSN format: protocol://[username:password@hostname:port]/path/to/folder
        Valid protocols are:
            imap, imaps, mbox, maildir
        Username is allowed to have @ (at) symbols in it
        Path should be the path for the mbox file, maildir directory, or the name
        of the IMAP folder
        """

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
            yield (msg_i, total_msgs, message)
            msg_i += 1

    def _copy_message(self, message, dest, mark_as_read=True):
        if mark_as_read:
            flags = r'(\Seen)'
        else:
            flags = None

        try:
            dest.append(self.dest_dsn['path'], flags, convert_date_header_to_tuple(message.get('Date')),
                message.as_string())
        except Exception as err:
            self.log.exception(err)


    def copy(self, progress_observer=None):
        self.log.info('Starting copy...')
        for i, total, message in self._iterate_messages(self.source):
            self.log.info('Copying message (%d/%d): %s -> %s @ %s', i, total,
                message.get('From'), message.get('Subject'), message.get('Date'))
            self._copy_message(message, self.dest)
        self.log.info('Copy finished')

    def copy_parallel(self, progress_observer=None, threads=4):
        self.log.debug('Starting threads')
        queue = Queue.Queue(maxsize=self.MAX_QUEUE_SIZE)
        thread_pool = [CopyThread(queue, self) for _ in xrange(threads)]
        for thread in thread_pool:
            thread.daemon = True
            thread.start()
            time.sleep(2)

        self.log.info('Starting copy...')
        self.log.info('Queueing %d messages...', len(self.source))
        for i, t, message in self._iterate_messages(self.source):
            queue.put(message)
        self.log.info('Queueing finished')
        queue.join()
        self.log.info('Copy finished')

class CopyThread(threading.Thread):
    def __init__(self, msg_queue, copier):
        threading.Thread.__init__(self)
        self._msg_queue = msg_queue
        self._copier = copier
        self.keep_running = True

    def run(self):
        err_count = 0
        dest = self._copier._create_target_imap4(self._copier.dest_dsn, True)
        if True:
            flags = r'(\Seen)'
        else:
            flags = None
        while self.keep_running:
            msg = self._msg_queue.get()
            try:
                dest.append(self._copier.dest_dsn['path'], flags,
                    convert_date_header_to_tuple(msg.get('Date')),
                    msg.as_string())
            except Exception as err:
                err_count += 1
                self._copier.log.error('Error (#%d) on message: %s -> %s @ %s',
                    err_count, msg.get('From'), msg.get('Subject'), msg.get('Date'))
                self._copier.log.exception(err)
                time.sleep(1)
                if err_count > 5:
                    self.keep_running = False
                    self._copier.log.warn('Max errors for this thread hit, exiting...')
            self._msg_queue.task_done()


if __name__ == '__main__':
    import optparse
    import multiprocessing

    parser = optparse.OptionParser()
    parser.add_option('-t', '--threads', dest='threads',
        help='Number of threads to run, 0=# of cores, 1=single threaded',
        type='int', default=0)
    parser.add_option('-q', '--quiet', action='store_true', default=False)
    parser.add_option('-v', '--verbose', action='store_true', default=False)

    options, args = parser.parse_args()

    copier = MailSyncer(args[0], args[1])

    if options.quiet:
        copier.log.setLevel(logging.WARN)
    elif options.verbose:
        copier.log.setLevel(logging.DEBUG)

    if options.threads == 0:
        thread_count = multiprocessing.cpu_count()
    elif options.threads == 1:
        thread_count = 1
    else:
        thread_count = options.threads

    if thread_count > 1:
        copier.copy_parallel(threads=thread_count)
    else:
        copier.copy()
