#!/usr/bin/env python2

import mailbox
import imaplib
import os.path
import datetime
import email.utils
import progressbar

DATETIME_FORMAT = '%a, %d %b %Y %H:%M:%S %z'
INTERNAL_DATE_FORMAT = '%d-%b-%Y %H:%M:%S +0000'

def convert_date_header(iso_date):
    date_tup = email.utils.parsedate_tz(iso_date)
    return (datetime.datetime(*date_tup[:6]) - datetime.timedelta(seconds=date_tup[-1])).utctimetuple()

def walk_mbox(mbox_filename):
    mbox = mailbox.mbox(mbox_filename, create=False)
    total_msgs = len(mbox)
    msg_i = 0
    for msg in mbox:
        yield (msg, convert_date_header(msg.get('date')), msg.as_string(), (msg_i, total_msgs))
        msg_i += 1

if __name__ == '__main__':
    import sys

    username, password = sys.argv[1].split(':')
    mbox_filename = sys.argv[2]
    upload_folder = sys.argv[3]

    GMAIL = imaplib.IMAP4_SSL('imap.gmail.com', imaplib.IMAP4_SSL_PORT)
    GMAIL.login(username, password)
    GMAIL.select(upload_folder)

    for msg, msg_date, msg_content, (msg_i, msg_t) in walk_mbox(mbox_filename):
        GMAIL.append(upload_folder, r'(\Seen)', msg_date, msg_content)
        print 'Uploaded [{2}/{3}]: {0} -> {1}'.format(msg.get('from'), msg.get('subject'), msg_i, msg_t)
