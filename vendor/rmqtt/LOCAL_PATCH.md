# Local RMQTT backport

This is the crates.io `rmqtt` 0.24.0 source (MIT OR Apache-2.0), with one fix
backported from upstream commit [`ad65cac60ee30f25c65518ae557a4efa4d8e3683`](https://github.com/rmqtt/rmqtt/commit/ad65cac60ee30f25c65518ae557a4efa4d8e3683).

The fix treats a dropped kick acknowledgement like the existing timeout path:
the old session is already gone, so the broker removes its stale peer entry
instead of refusing a valid reconnect with MQTT 3.1.1 CONNACK `0x03`. Do not
remove this patch until a pinned upstream RMQTT release includes the fix and
the takeover-race test passes against that release.
