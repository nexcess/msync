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
import socket
import collections

class Dequeue(collections.deque):
    """A collections.deque that looks like a Queue.Queue
    """

    MICROSLEEP_STEP = 0.01

    def __init__(self, maxsize=None):
        collections.deque.__init__(self)
        self.__queue_maxsize = maxsize

    def __now(self):
        return time.time()

    def __wait(self):
        time.sleep(self.MICROSLEEP_STEP)

    def qsize(self):
        return len(self)

    def empty(self):
        return len(self) == 0

    def full(self):
        return len(self) == self.__queue_maxsize \
            if self.__queue_maxsize is not None else False

    def put(self, item, block=True, timeout=None): #Full
        if block:
            if timeout is not None:
                start = self.__now()
                while not self.full():
                    if self.__now() > start + timeout:
                        raise Queue.Full()
                    self.__wait()
                self.appendleft(item)

            else:
                while not self.full():
                    self.__wait()
                self.appendleft(item)
        else:
            if not self.full():
                self.appendleft(item)
            else:
                raise Queue.Full()

    def put_nowait(self, item):
        self.put(item, False)

    def get(self, block=True, timeout=None): #IndexError -> Empty
        if block:
            if timeout is not None:
                start = self.__now()
                while True:
                    try:
                        return self.pop()
                    except IndexError as qerr:
                        if self.__now() > start + timeout:
                            raise Queue.Empty(qerr)
                    self.__wait()
            else:
                while True:
                    try:
                        return self.pop()
                    except IndexError as qerr:
                        self.__wait()
        else:
            try:
                return self.pop()
            except IndexError as qerr:
                raise Queue.Empty(qerr)

    def get_nowait(self):
        return self.get(False)

    def task_done(self): pass

    def join(self):
        while not self.empty():
            self.__wait()

def convert_date_header_to_tuple(date_str):
    """Convert a date from $DATE_HEADER_FORMAT to a timestruct tuple that the
    imap APPEND command will accept
    """

    # this may also work
    # imaplib.Time2Internaldate(email.utils.parsedate_tz(date_str)[:-1])
    if date_str is not None:
        parsed_date = email.utils.parsedate_tz(date_str)
        timestamp = datetime.datetime(*parsed_date[:6])
        if parsed_date[-1]:
             timestamp = timestamp - datetime.timedelta(seconds=parsed_date[-1])
        timetuple = timestamp.utctimetuple()
    else:
        timetuple = datetime.datetime.now().utctimetuple()
    return timetuple

def wait(seconds):
    """Sleep for at least $seconds, plus a random amount of milliseconds (NYI)
    """
    time.sleep(seconds)

class MailSyncer(object):
    DATE_HEADER_FORMAT      = '%a, %d %b %Y %H:%M:%S %z'
    INTERNALDATE_FORMAT     = '%d-%b-%Y %H:%M:%S +0000'
    MAX_QUEUE_SIZE          = 1024 * 8
    PROGRESS_STEP_SIZE      = 15

    def __init__(self, source_dsn, dest_dsn):
        self.log = self._create_logger()
        self.source_dsn = self._parse_dsn(source_dsn)
        self.dest_dsn = self._parse_dsn(dest_dsn)

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
        target = {
            'imap': lambda dsn: self._create_target_imap4(dsn, False),
            'imaps': lambda dsn: self._create_target_imap4(dsn, True),
            'mbox': self._create_target_mbox,
            'maildir': self._create_target_maildir,
        }.get(dsn['type'])(dsn)
        return target

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

    def _spawn_thread(self, queue):
        thread = CopyThread(queue, self)
        thread.daemon = True
        thread.start()
        return thread

    def copy(self, progress_observer=None):
        source = self._create_target(source_dsn)
        dest = self._create_target(dest_dsn)
        self.log.info('Starting copy...')
        for i, total, message in self._iterate_messages(source):
            self.log.info('Copying message (%d/%d): %s -> %s @ %s', i, total,
                message.get('From'), message.get('Subject'), message.get('Date'))
            self._copy_message(message, dest)
        self.log.info('Copy finished')

    def copy_parallel(self, progress_observer=None, threads=4):
        source = self._create_target(self.source_dsn)

        self.log.debug('Starting threads')
        queue = Queue.Queue(maxsize=self.MAX_QUEUE_SIZE)
        for _ in xrange(threads):
            self._spawn_thread(queue)
            time.sleep(2)

        self.log.info('Found %d messages to copy', len(source))
        for i, t, message in self._iterate_messages(source):
            # TODO: check if any threads still alive
            queue.put((message, 0),)
            if i % self.PROGRESS_STEP_SIZE == 0:
                self.log.info('Added messages to queue: %d/%d', i, t)
        self.log.debug('Queueing finished')
        while not queue.empty():
            # TODO: check if any threads still alive
            self.log.info('Messages left in queue: ~%d/%d', queue.qsize(), self.MAX_QUEUE_SIZE)
            time.sleep(self.PROGRESS_STEP_SIZE)
        queue.join()
        self.log.info('Copy finished')

class CopyThread(threading.Thread):
    MAX_IMAP_ERRORS     = 3
    MAX_UNKN_ERRORS     = 5
    MAX_MSG_RETRIES     = 3

    def __init__(self, msg_queue, copier):
        threading.Thread.__init__(self)
        self._msg_queue = msg_queue
        self._copier = copier
        self.log = self._copier.log
        self.keep_running = True

    def _create_target(self):
        for i in xrange(self.MAX_IMAP_ERRORS):
            try:
                target = self._copier._create_target(self._copier.dest_dsn)
            except imaplib.IMAP4.error as err:
                self.log.warn('Error when attempting to login, attempt #%d: %s', i+1, err)
                time.sleep(2)
            else:
                break
        else:
            self.log.error('Failed to login after %d attempts, giving up',
                self.MAX_IMAP_ERRORS)
            raise Exception('Login attempt(s) failed, giving up')
        return target

    def _requeue_msg(self, message, cur_retries):
        if cur_retries >= self.MAX_MSG_RETRIES:
            self.log.warn('Message retried too many times, dropping it: %s -> %s @ %s',
                message.get('From'), message.get('Subject'), message.get('Date'))
        else:
            self._msg_queue.put((message, cur_retries + 1),)

    def run(self):
        self.log.debug('Thread (%s) run() started', self.ident)
        unkn_err_count = 0
        imap_err_count = 0
        dest = self._create_target()
        dest_path = self._copier.dest_dsn['path']
        if True:
            flags = r'(\Seen)'
        else:
            flags = None
        while self.keep_running:
            msg, msg_retries = self._msg_queue.get()
            self._msg_queue.task_done()
            try:
                dest.append(dest_path, flags,
                    convert_date_header_to_tuple(msg.get('Date')),
                    msg.as_string())
            except socket.error as err:
                self.log.warn('Socket error on thread(%s): %s', self.ident, err)
                self.log.debug('Thread respawning...')
                self._copier._spawn_thread(self._msg_queue)
                self._requeue_msg(msg, msg_retries)
                self.keep_running = False
            except imaplib.IMAP4.error as err:
                imap_err_count += 1
                self.log.warn('IMAP error (#%d) on message: %s -> %s @ %s',
                    imap_err_count, msg.get('From'), msg.get('Subject'), msg.get('Date'))
                self.log.exception(err)
                time.sleep(1)
                self.log.debug('Reconnecting to IMAP server and requeueing message')
                if imap_err_count >= self.MAX_IMAP_ERRORS:
                    self.log.warn('Max IMAP errors for this thread hit, respawning...')
                    self._copier._spawn_thread(self._msg_queue)
                    self.keep_running = False
                else:
                    dest = self._create_target()
                # cheap way to skip requeueing messages that are too large
                if not 'too large' in str(err):
                    self._requeue_msg(msg, msg_retries)
            except Exception as err:
                unkn_err_count += 1
                self.log.warn('Unknown error (#%d) on message: %s -> %s @ %s',
                    unkn_err_count, msg.get('From'), msg.get('Subject'), msg.get('Date'))
                self.log.exception(err)
                time.sleep(1)
                if unkn_err_count >= self.MAX_UNKN_ERRORS:
                    self.log.error('Max unknown errors for this thread hit, exiting...')
                    self.keep_running = False
                self._requeue_msg(msg, msg_retries)

        self.log.debug('Thread (%s) run() stopped', self.ident)

if __name__ == '__main__':
    import optparse
    import multiprocessing

    parser = optparse.OptionParser()
    parser.add_option('-t', '--threads',
        type='int', default=0,
        help='Number of threads to run, 0=# of cores, 1=single threaded')
    parser.add_option('-q', '--quiet',
        action='store_true', default=False)
    parser.add_option('-v', '--verbose',
        action='store_true', default=False)
    parser.add_option('-s', '--queue-size',
        type='int', default=MailSyncer.MAX_QUEUE_SIZE,
        help='Max mails to queue at once, reduce this to lower the amount of memory used')
    options, args = parser.parse_args()

    copier = MailSyncer(args[0], args[1])

    copier.MAX_QUEUE_SIZE = max(options.queue_size, 256)

    if options.quiet:
        copier.log.setLevel(logging.WARN)
    elif options.verbose:
        copier.log.setLevel(logging.DEBUG)

    if options.threads == 0:
        thread_count = multiprocessing.cpu_count()
    elif options.threads == 1:
        thread_count = 1
    else:
        thread_count = max(options.threads, 1)

    if thread_count > 1:
        copier.copy_parallel(threads=thread_count)
    else:
        copier.copy()
