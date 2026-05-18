import type { HealthResponse, LeaderboardResponse } from '../lib/types';

type Props = {
  health: HealthResponse | null;
  data: LeaderboardResponse | null;
};

export function SummaryCards({ health, data }: Props) {
  const top = data?.leaderboard[0];
  const avgEvents =
    data && data.leaderboard.length
      ? Math.round(
          data.leaderboard.reduce((s, r) => s + r.n_pressing_events, 0) /
            data.leaderboard.length,
        )
      : null;

  return (
    <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
      <Card
        kicker="Tournament"
        value={data?.competition ?? '—'}
        sub={data?.season ? `Season ${data.season}` : ' '}
        accent
      />
      <Card
        kicker="Qualifying players"
        value={data ? String(data.total_qualifying_players) : '—'}
        sub="≥ 100 pressing events"
      />
      <Card
        kicker="Top PAP this season"
        value={top ? top.player_name : '—'}
        sub={top ? `${top.team} · +${top.pap_score.toFixed(3)}` : ' '}
      />
      <Card
        kicker="Avg pressing events"
        value={avgEvents !== null ? String(avgEvents) : '—'}
        sub={
          health
            ? `Trained on ${health.n_training_events.toLocaleString()} events`
            : ' '
        }
      />
    </div>
  );
}

function Card({
  kicker,
  value,
  sub,
  accent,
}: {
  kicker: string;
  value: string;
  sub: string;
  accent?: boolean;
}) {
  return (
    <div className="panel rounded-lg p-4 relative overflow-hidden">
      {accent && (
        <div className="absolute inset-y-0 left-0 w-[3px] bg-pitch/80" />
      )}
      <div className="kicker">{kicker}</div>
      <div className="mt-2 text-xl font-medium text-chalk truncate">{value}</div>
      <div className="mt-1 text-xs text-ink-300 truncate">{sub}</div>
    </div>
  );
}
