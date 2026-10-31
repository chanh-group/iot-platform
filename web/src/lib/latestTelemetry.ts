import type { LatestValue } from "./models";
import type { LiveTelemetry } from "./chart";

interface LatestCandidate {
  value: LatestValue;
  receivedAt?: number;
}

export interface AmbiguousLatestGroup {
  key: string;
  ts: number;
  receivedAt: number | undefined;
  eventIds: string[];
}

export interface LatestMerge {
  items: LatestValue[];
  ambiguous: AmbiguousLatestGroup[];
}

export function mergeLatestValues(rest: LatestValue[], live: LiveTelemetry[]): LatestMerge {
  const latestByKey = new Map<string, LatestCandidate>();
  const ambiguous = new Map<string, { key: string; ts: number; receivedAt: number | undefined; eventIds: Set<string> }>();
  const seenEvents = new Set<string>();
  for (const value of rest) {
    latestByKey.set(value.key, {
      value,
      receivedAt: value.received_at,
    });
  }

  const markAmbiguous = (
    key: string,
    ts: number,
    receivedAt: number | undefined,
    eventIds: (string | undefined)[],
  ) => {
    const signature = `${key}\0${ts}\0${receivedAt ?? "unknown"}`;
    let group = ambiguous.get(signature);
    if (!group) {
      group = { key, ts, receivedAt, eventIds: new Set() };
      ambiguous.set(signature, group);
    }
    for (const eventId of eventIds) if (eventId) group.eventIds.add(eventId);
  };

  for (const event of live) {
    if (seenEvents.has(event.event_id)) continue;
    seenEvents.add(event.event_id);
    for (const [key, rawValue] of Object.entries(event.values)) {
      const incoming: LatestValue = {
        key,
        value: rawValue,
        ts: event.ts,
        received_at: event.received_at ?? event.ts,
        event_id: event.event_id,
      };
      const current = latestByKey.get(key);
      if (!current || incoming.ts > current.value.ts) {
        latestByKey.set(key, { value: incoming, receivedAt: event.received_at });
        continue;
      }
      if (incoming.ts < current.value.ts) continue;

      const incomingReceivedAt = event.received_at;
      const currentReceivedAt = current.receivedAt;
      if (incomingReceivedAt === undefined || currentReceivedAt === undefined) {
        markAmbiguous(key, event.ts, incomingReceivedAt, [event.event_id, current.value.event_id]);
      } else if (incomingReceivedAt > currentReceivedAt) {
        latestByKey.set(key, { value: incoming, receivedAt: incomingReceivedAt });
      } else if (
        incomingReceivedAt === currentReceivedAt &&
        incoming.event_id !== current.value.event_id
      ) {
        markAmbiguous(key, event.ts, incomingReceivedAt, [event.event_id, current.value.event_id]);
      }
    }
  }

  return {
    items: [...latestByKey.values()].map((candidate) => candidate.value).sort((left, right) => left.key.localeCompare(right.key)),
    ambiguous: [...ambiguous.values()].map((group) => ({
      key: group.key,
      ts: group.ts,
      receivedAt: group.receivedAt,
      eventIds: [...group.eventIds],
    })),
  };
}
