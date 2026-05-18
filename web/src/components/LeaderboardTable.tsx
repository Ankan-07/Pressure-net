import { useMemo, useState } from 'react';
import type { LeaderboardRow } from '../lib/types';
import { fmtPap, fmtPct, initials } from '../lib/format';
import { PapBar } from './PapBar';

type Props = {
  rows: LeaderboardRow[];
  loading: boolean;
  onRowClick?: (r: LeaderboardRow) => void;
};

type SortKey = 'rank' | 'player_name' | 'team' | 'pap_score' | 'pap_percentile' | 'n_pressing_events';
type SortDir = 'asc' | 'desc';

const DEFAULT_SORT: { key: SortKey; dir: SortDir } = { key: 'rank', dir: 'asc' };

export function LeaderboardTable({ rows, loading, onRowClick }: Props) {
  const [sort, setSort] = useState(DEFAULT_SORT);

  const sorted = useMemo(() => {
    const arr = [...rows];
    const dir = sort.dir === 'asc' ? 1 : -1;
    arr.sort((a, b) => {
      const va = a[sort.key];
      const vb = b[sort.key];
      if (typeof va === 'number' && typeof vb === 'number') return (va - vb) * dir;
      return String(va).localeCompare(String(vb)) * dir;
    });
    return arr;
  }, [rows, sort]);

  const maxAbsPap = Math.max(0.001, ...rows.map((r) => Math.abs(r.pap_score)));

  function toggle(key: SortKey, defaultDir: SortDir = 'desc') {
    setSort((s) =>
      s.key === key
        ? { key, dir: s.dir === 'asc' ? 'desc' : 'asc' }
        : { key, dir: defaultDir },
    );
  }

  return (
    <div className="panel rounded-lg overflow-hidden">
      <div className="px-4 py-3 flex items-center border-b hairline">
        <h2 className="font-display tracking-wider2 text-chalk text-lg">PAP LEADERBOARD</h2>
        <span className="ml-3 pill-pitch">live</span>
        <span className="ml-auto text-xs text-ink-300">
          Pressure-Adjusted Performance · per player
        </span>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-ink-300 bg-ink-850/60">
              <Th className="w-12 text-center" sortKey="rank"             sort={sort} onSort={toggle} defaultDir="asc">#</Th>
              <Th sortKey="player_name"      sort={sort} onSort={toggle} defaultDir="asc">Player</Th>
              <Th sortKey="team"             sort={sort} onSort={toggle} defaultDir="asc">Team</Th>
              <Th className="text-right"     sortKey="pap_score"          sort={sort} onSort={toggle}>PAP</Th>
              <Th className="w-32">vs baseline</Th>
              <Th className="text-right w-24" sortKey="pap_percentile"    sort={sort} onSort={toggle}>Pctl</Th>
              <Th className="text-right w-24" sortKey="n_pressing_events" sort={sort} onSort={toggle}>Events</Th>
            </tr>
          </thead>
          <tbody>
            {loading && rows.length === 0 && <SkeletonRows />}
            {!loading && rows.length === 0 && (
              <tr>
                <td colSpan={7} className="px-4 py-10 text-center text-ink-300">
                  No qualifying players for the current filters.
                </td>
              </tr>
            )}
            {sorted.map((r) => (
              <Row
                key={r.player_id}
                r={r}
                maxAbsPap={maxAbsPap}
                onClick={onRowClick ? () => onRowClick(r) : undefined}
              />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function Th({
  children,
  className = '',
  sortKey,
  sort,
  onSort,
  defaultDir = 'desc',
}: {
  children: React.ReactNode;
  className?: string;
  sortKey?: SortKey;
  sort?: { key: SortKey; dir: SortDir };
  onSort?: (k: SortKey, d?: SortDir) => void;
  defaultDir?: SortDir;
}) {
  const sortable = !!sortKey && !!onSort;
  const active = sortable && sort?.key === sortKey;
  const dir = active ? sort!.dir : null;

  const isRight = className.includes('text-right');

  return (
    <th
      className={
        'px-4 py-2.5 font-medium text-[11px] uppercase tracking-wider2 ' +
        (sortable ? 'cursor-pointer select-none hover:text-chalk ' : '') +
        className
      }
      onClick={sortable ? () => onSort!(sortKey!, defaultDir) : undefined}
    >
      <span className={['inline-flex items-center gap-1', isRight ? 'justify-end w-full' : ''].join(' ')}>
        <span className={active ? 'text-chalk' : ''}>{children}</span>
        {sortable && (
          <SortArrow dir={dir} />
        )}
      </span>
    </th>
  );
}

function SortArrow({ dir }: { dir: SortDir | null }) {
  if (dir === null) {
    return (
      <svg viewBox="0 0 12 12" className="w-3 h-3 text-ink-500" fill="currentColor">
        <path d="M6 3l3 4H3l3-4z" opacity=".4" />
        <path d="M6 9l3-4H3l3 4z" opacity=".4" />
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 12 12" className="w-3 h-3 text-pitch" fill="currentColor">
      {dir === 'asc' ? <path d="M6 3l3 4H3l3-4z" /> : <path d="M6 9l3-4H3l3 4z" />}
    </svg>
  );
}

function Row({
  r,
  maxAbsPap,
  onClick,
}: {
  r: LeaderboardRow;
  maxAbsPap: number;
  onClick?: () => void;
}) {
  const positive = r.pap_score >= 0;
  return (
    <tr
      onClick={onClick}
      className={[
        'border-t hairline hover:bg-ink-850/60 transition-colors',
        onClick ? 'cursor-pointer' : '',
      ].join(' ')}
    >
      <td className="px-4 py-3 text-center stat-num text-ink-300">
        {r.rank <= 3 ? (
          <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-pitch/10 text-pitch border border-pitch/30 text-xs font-semibold">
            {r.rank}
          </span>
        ) : (
          r.rank
        )}
      </td>
      <td className="px-4 py-3">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-full bg-ink-800 border hairline flex items-center justify-center text-[11px] font-semibold text-ink-200">
            {initials(r.player_name)}
          </div>
          <div className="min-w-0">
            <div className="text-chalk font-medium truncate">{r.player_name}</div>
            <div className="text-[11px] text-ink-300 stat-num">id · {r.player_id}</div>
          </div>
        </div>
      </td>
      <td className="px-4 py-3 text-ink-200">{r.team}</td>
      <td className={[
        'px-4 py-3 text-right stat-num font-semibold',
        positive ? 'text-pitch' : 'text-booking',
      ].join(' ')}>
        {fmtPap(r.pap_score)}
      </td>
      <td className="px-4 py-3">
        <PapBar value={r.pap_score} max={maxAbsPap} />
      </td>
      <td className="px-4 py-3 text-right stat-num text-ink-100">{fmtPct(r.pap_percentile)}</td>
      <td className="px-4 py-3 text-right stat-num text-ink-200">{r.n_pressing_events}</td>
    </tr>
  );
}

function SkeletonRows() {
  return (
    <>
      {Array.from({ length: 8 }).map((_, i) => (
        <tr key={i} className="border-t hairline">
          {Array.from({ length: 7 }).map((_, j) => (
            <td key={j} className="px-4 py-3">
              <div className="h-3 rounded bg-ink-800 animate-pulse" />
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}
