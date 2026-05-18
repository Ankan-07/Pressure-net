import { Link } from 'react-router-dom';
import type { PlayerRating } from '../lib/types';
import { fmtPap, initials } from '../lib/format';

type Props = { p: PlayerRating };

export function PlayerHero({ p }: Props) {
  const positive = p.pap_score >= 0;
  return (
    <div className="panel rounded-lg p-6 relative overflow-hidden">
      <div
        className="absolute inset-y-0 left-0 w-[3px]"
        style={{ backgroundColor: positive ? '#7CFF6B' : '#E5484D' }}
      />

      <div className="flex flex-wrap items-start gap-6">
        <div className="w-20 h-20 rounded-full bg-ink-800 border hairline flex items-center justify-center text-2xl font-semibold text-chalk shrink-0">
          {initials(p.player_name)}
        </div>

        <div className="min-w-0 flex-1">
          <Link
            to="/"
            className="kicker text-ink-300 hover:text-pitch transition-colors inline-block"
          >
            ← Leaderboard
          </Link>
          <h1 className="font-display tracking-wider2 text-chalk text-4xl leading-none mt-1">
            {p.player_name.toUpperCase()}
          </h1>
          <div className="mt-2 flex flex-wrap items-center gap-2 text-sm text-ink-200">
            <span>{p.team}</span>
            <span className="text-ink-500">·</span>
            <span>{p.competition}</span>
            <span className="text-ink-500">·</span>
            <span>{p.season}</span>
            <span className="ml-2 pill-ink stat-num">id {p.player_id}</span>
          </div>
        </div>

        <div className="flex items-stretch gap-3 ml-auto">
          <HeroStat kicker="PAP score" value={fmtPap(p.pap_score)} tone={positive ? 'pitch' : 'booking'} big />
          <HeroStat kicker="Percentile" value={p.pap_percentile.toFixed(1)} />
          <HeroStat kicker="Events" value={String(p.n_pressing_events)} />
        </div>
      </div>
    </div>
  );
}

function HeroStat({
  kicker,
  value,
  tone,
  big,
}: {
  kicker: string;
  value: string;
  tone?: 'pitch' | 'booking';
  big?: boolean;
}) {
  const color =
    tone === 'pitch' ? 'text-pitch' : tone === 'booking' ? 'text-booking' : 'text-chalk';
  return (
    <div className="flex flex-col items-end justify-center px-4 py-2 border-l hairline">
      <div className="kicker">{kicker}</div>
      <div className={['mt-1 stat-num font-semibold', big ? 'text-4xl' : 'text-2xl', color].join(' ')}>
        {value}
      </div>
    </div>
  );
}
