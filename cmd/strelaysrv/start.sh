#!/bin/sh

app \
	-keys=/keys \
	-listen="${LISTEN:-:443}" \
	-provided-by="${PROVIDEDBY:-Syncthing Docker Image}" \
	-global-rate="${GLOBALRATE:-10000}" \
	-per-session-rate="${SESSIONRATE:-1000}"

