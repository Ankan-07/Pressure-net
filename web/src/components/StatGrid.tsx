import type { PlayerRating } from '../lib/types';

type Props = { p: PlayerRating };

export function StatGrid({ p }: Props) {
  const delta = p.success_rate - p.expected_success;
  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
      <Stat
        kicker="Success rate"
        value={`${(p.success_rate * 100).toFixed(1)}%`}
        sub="actual outcomes"
      />
      <Stat
        kicker="Expected success"
        value={`${(p.expected_success * 100).toFixed(1)}%`}
        sub="model baseline"
      />
      <Stat
        kicker="Over-performance"
        value={`${delta >= 0 ? '+' : '−'}${(Math.abs(delta) * 100).toFixed(1)} pp`}
        sub="actual − expected"
        tone={delta >= 0 ? 'pitch' : 'booking'}
      />
      <Stat
        kicker="Avg difficulty"
        value={p.avg_difficulty_faced.toFixed(3)}
        sub="model-estimated"
      />
    </div>
  );
}

function Stat({
  kicker,
  value,
  sub,
  tone,
}: {
  kicker: string;
  value: string;
  sub: string;
  tone?: 'pitch' | 'booking';
}) {
  const color =
    tone === 'pitch' ? 'text-pitch' : tone === 'booking' ? 'text-booking' : 'text-chalk';
  return (
    <div className="panel rounded-lg p-4">
      <div className="kicker">{kicker}</div>
      <div className={`mt-2 text-2xl font-medium stat-num ${color}`}>{value}</div>
      <div className="mt-1 text-xs text-ink-300">{sub}</div>
    </div>
  );
}
