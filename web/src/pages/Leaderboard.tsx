import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { SummaryCards } from '../components/SummaryCards';
import { FilterBar } from '../components/FilterBar';
import { LeaderboardTable } from '../components/LeaderboardTable';
import { getLeaderboard } from '../lib/api';
import { COMPETITIONS, type Competition, type HealthResponse, type LeaderboardResponse } from '../lib/types';

type Props = { health: HealthResponse | null };

export default function LeaderboardPage({ health }: Props) {
  const navigate = useNavigate();
  const [data, setData] = useState<LeaderboardResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [competition, setCompetition] = useState<Competition>(COMPETITIONS[0]);
  const [minEvents, setMinEvents] = useState(100);
  const [topN, setTopN] = useState(50);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    const params: Parameters<typeof getLeaderboard>[0] = {
      min_events: minEvents,
      top_n: topN,
    };
    if (competition.key !== 'all') {
      params.competition = competition.competition_id;
      params.season = competition.season_id;
    }
    getLeaderboard(params)
      .then((r) => { if (!cancelled) setData(r); })
      .catch((e) => { if (!cancelled) setError(String(e.message ?? e)); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [competition, minEvents, topN]);

  return (
    <div className="max-w-[1280px] mx-auto px-6 py-6 space-y-5">
      <PageIntro />
      <SummaryCards health={health} data={data} />
      <FilterBar
        selected={competition}
        onSelect={setCompetition}
        minEvents={minEvents}
        onMinEvents={setMinEvents}
        topN={topN}
        onTopN={setTopN}
      />

      {error && <ApiError msg={error} />}

      <LeaderboardTable
        rows={data?.leaderboard ?? []}
        loading={loading}
        onRowClick={(r) => navigate(`/players/${r.player_id}`)}
      />
    </div>
  );
}

function PageIntro() {
  return (
    <div className="flex items-end justify-between gap-4 pt-2">
      <div>
        <div className="kicker">Week 6 · ranking engine</div>
        <h2 className="mt-1 font-display text-4xl tracking-wider2 text-chalk leading-none">
          WHO PLAYS BEST UNDER PRESSURE?
        </h2>
        <p className="mt-2 text-sm text-ink-300 max-w-2xl">
          PAP measures performance versus an{' '}
          <span className="text-ink-100">average player in the same pressing situation</span>{' '}
          — based on StatsBomb 360 freeze-frames, scored by a calibrated Transformer
          over 3-step temporal context.
        </p>
      </div>
      <div className="hidden md:flex flex-col items-end gap-1">
        <span className="pill-whistle">EURO 2024 · 2020 · WC 2022</span>
        <span className="pill-ink">T=3 · F=15 · d=64</span>
      </div>
    </div>
  );
}

function ApiError({ msg }: { msg: string }) {
  return (
    <div className="panel rounded-lg p-4 border-booking/40 text-booking">
      <div className="kicker text-booking">API error</div>
      <div className="mt-1 text-sm">{msg}</div>
      <div className="mt-2 text-xs text-ink-300">
        Start the API with{' '}
        <code className="px-1.5 py-0.5 bg-ink-800 rounded text-ink-100">
          uv run uvicorn api.main:app --reload --port 8765
        </code>
      </div>
    </div>
  );
}
