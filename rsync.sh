#!/bin/bash
rsync --exclude '*.txt' -avxtz ../fbscrape www.malioglasi.rs:~/
rsync --exclude '*.txt' -avxtz ../fbscrape guyromm:~/