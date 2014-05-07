msync
=====

rsync for IMAP4, mbox, and maildir

**NOTE**: msync is a work-in-progress, currently it can only transfer from
mbox/maildir to IMAP. Eventually it will be able to transfer between
any of the supported formats.

## Usage

The arguments are modelled after `rsync` somewhat, both *source* and *destination*
take the form of `protocol://[username:password@host:port]/folder`. For example, to
copy from a local MBOX file to an IMAP server:

    msync.py 'mbox://./sent_mail.mbox' 'imap://me@example.com:abc123@imap.example.com:143/Sent Mail'

