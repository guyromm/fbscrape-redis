#!/bin/bash
while (true) ; do ssh -R localhost:6379:localhost:6379 $1 'cd fbscrape; ./fbscrape.py scrape[$2]' ; sleep 60 ; done
