#!/bin/bash
exec 1>&7
while :; do echo '{"test": true}'; sleep 0.5; done
