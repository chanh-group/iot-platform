//! Device ACL matrix (`02` §2.3–§2.4), default deny.
//!
//! Pure functions: the MQTT adapter maps RMQTT sessions and publish/subscribe
//! parameters in, nothing here knows about the broker.

/// Contract cap for device topics (`02` §2.3), independent of the broker setting.
const MAX_TOPIC_LEVELS: usize = 16;

/// Everything after `devices/{device_id}/`, or `None` when the prefix differs.
fn device_suffix<'a>(device_id: &str, topic: &'a str) -> Option<&'a str> {
    if device_id.is_empty() {
        return None;
    }
    let rest = topic.strip_prefix("devices/")?.strip_prefix(device_id)?;
    rest.strip_prefix('/')
}

fn topic_levels(topic: &str) -> usize {
    topic.split('/').count()
}

/// A device may publish only to its own concrete business topics.
#[must_use]
pub fn can_publish(device_id: &str, topic: &str) -> bool {
    if topic.contains('+') || topic.contains('#') || topic_levels(topic) > MAX_TOPIC_LEVELS {
        return false;
    }
    let Some(suffix) = device_suffix(device_id, topic) else {
        return false;
    };
    matches!(
        suffix,
        "telemetry" | "attributes" | "commands/ack" | "status"
    ) || suffix
        .strip_prefix("data/")
        .is_some_and(|rest| !rest.is_empty())
}

/// A device may read only its own server topics and its `data/` subtree.
#[must_use]
pub fn can_subscribe(device_id: &str, filter: &str) -> bool {
    if filter.starts_with('$') || topic_levels(filter) > MAX_TOPIC_LEVELS {
        return false;
    }
    let Some(suffix) = device_suffix(device_id, filter) else {
        return false;
    };
    if matches!(suffix, "telemetry/ack" | "commands" | "status") {
        return true;
    }
    suffix
        .strip_prefix("data/")
        .is_some_and(|rest| !rest.is_empty() && valid_filter(rest))
}

/// Wildcards are allowed under `data/` only as whole levels, with `#` last.
fn valid_filter(rest: &str) -> bool {
    let levels: Vec<&str> = rest.split('/').collect();
    levels.iter().enumerate().all(|(index, level)| {
        if *level == "#" {
            return index + 1 == levels.len();
        }
        !level.contains('#') && (*level == "+" || !level.contains('+'))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "019c2f7e-0000-7000-8000-00000000000a";
    const B: &str = "019c2f7e-0000-7000-8000-00000000000b";

    fn own(device_id: &str, suffix: &str) -> String {
        format!("devices/{device_id}/{suffix}")
    }

    #[test]
    fn publish_matrix() {
        for (suffix, expected) in [
            ("telemetry", true),
            ("attributes", true),
            ("commands/ack", true),
            ("status", true),
            ("data/x", true),
            ("data/x/y/z", true),
            ("telemetry/ack", false),
            ("commands", false),
            ("data/", false),
            ("telemetry/x", false),
            ("", false),
            ("telemetry/+", false),
            ("telemetry/#", false),
            ("data/x/#", false),
            ("data/+/x", false),
            ("../telemetry", false),
        ] {
            assert_eq!(
                can_publish(A, &own(A, suffix)),
                expected,
                "publish {suffix:?}"
            );
        }
        let max_data = format!("devices/{A}/data/{}", vec!["x"; 13].join("/"));
        assert!(can_publish(A, &max_data), "16 levels are allowed");
        let too_deep = format!("devices/{A}/data/{}", vec!["x"; 14].join("/"));
        assert!(!can_publish(A, &too_deep), "17 levels are denied");
    }

    #[test]
    fn publish_is_denied_outside_the_device_prefix() {
        for topic in [
            own(B, "telemetry"),
            own(B, "data/x"),
            "devices/telemetry".to_owned(),
            format!("devices/{A}"),
            format!("devices/{A}x/telemetry"),
            format!("devices//{A}/telemetry"),
            "$SYS/brokers".to_owned(),
            format!("$share/g/{}", own(A, "telemetry")),
        ] {
            assert!(!can_publish(A, &topic), "publish {topic:?}");
        }
    }

    #[test]
    fn subscribe_matrix() {
        for (suffix, expected) in [
            ("telemetry/ack", true),
            ("commands", true),
            ("status", true),
            ("data/#", true),
            ("data/+", true),
            ("data/sensors/+", true),
            ("data/sensors/#", true),
            ("data/sensors/room", true),
            ("telemetry", false),
            ("attributes", false),
            ("commands/ack", false),
            ("data/", false),
            ("data/a#", false),
            ("data/a+/b", false),
            ("data/#/x", false),
            ("", false),
            ("#", false),
            ("#/devices", false),
        ] {
            assert_eq!(
                can_subscribe(A, &own(A, suffix)),
                expected,
                "subscribe {suffix:?}"
            );
        }
    }

    #[test]
    fn subscribe_is_denied_outside_the_device_prefix() {
        for filter in [
            own(B, "commands"),
            own(B, "data/#"),
            "devices/+/telemetry".to_owned(),
            "devices/#".to_owned(),
            format!("devices/{A}"),
            format!("devices/{A}/#"),
            "$SYS/#".to_owned(),
            "$share/g/".to_owned() + &own(A, "data/#"),
            format!("devices/{A}/data/{}", "x/".repeat(20)) + "#",
        ] {
            assert!(!can_subscribe(A, &filter), "subscribe {filter:?}");
        }
    }

    #[test]
    fn empty_principal_matches_nothing() {
        assert!(!can_publish("", "devices//telemetry"));
        assert!(!can_publish("", "/telemetry"));
        assert!(!can_subscribe("", "devices//data/#"));
        assert!(!can_subscribe("", "$SYS/#"));
    }
}
