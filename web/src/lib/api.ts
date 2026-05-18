import type {
  EvaluateEventResponse,
  HealthResponse,
  LeaderboardResponse,
  PlayerAttention,
  PlayerRating,
  SearchResponse,
} from './types';

const BASE = '/api/v1';

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText} — ${path}`);
  }
  return res.json() as Promise<T>;
}

export function getHealth() {
  return get<HealthResponse>('/health');
}

export function getLeaderboard(params: {
  competition?: number;
  season?: number;
  min_events?: number;
  top_n?: number;
}) {
  const q = new URLSearchParams();
  if (params.competition) q.set('competition', String(params.competition));
  if (params.season)      q.set('season', String(params.season));
  if (params.min_events)  q.set('min_events', String(params.min_events));
  if (params.top_n)       q.set('top_n', String(params.top_n));
  const qs = q.toString();
  return get<LeaderboardResponse>(`/leaderboard${qs ? `?${qs}` : ''}`);
}

export function getPlayerRating(playerId: number) {
  return get<PlayerRating>(`/player/${playerId}/rating`);
}

export function getPlayerAttention(playerId: number) {
  return get<PlayerAttention>(`/player/${playerId}/attention`);
}

export function searchPlayers(q: string, limit = 8) {
  return get<SearchResponse>(`/players/search?q=${encodeURIComponent(q)}&limit=${limit}`);
}

export async function evaluateEvent(features: number[][]): Promise<EvaluateEventResponse> {
  const res = await fetch(`${BASE}/evaluate-event`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ features }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`${res.status} ${res.statusText} — ${text}`);
  }
  return res.json() as Promise<EvaluateEventResponse>;
}
