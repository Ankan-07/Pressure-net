import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { PlayerHero } from '../components/PlayerHero';
import { StatGrid } from '../components/StatGrid';
import { PitchZone } from '../components/PitchZone';
import { AttentionHeatmap } from '../components/AttentionHeatmap';
import { getPlayerAttention, getPlayerRating } from '../lib/api';
import type { PlayerAttention, PlayerRating } from '../lib/types';

export default function PlayerDetailPage() {
  const { playerId } = useParams<{ playerId: string }>();
  const id = Number(playerId);

  const [rating, setRating] = useState<PlayerRating | null>(null);
  const [attn, setAttn] = useState<PlayerAttention | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    setRating(null);
    setAttn(null);

    Promise.all([
      getPlayerRating(id).catch((e) => { throw new Error(`rating: ${e.message ?? e}`); }),
      getPlayerAttention(id).catch(() => null),
    ])
      .then(([r, a]) => {
        if (cancelled) return;
        setRating(r);
        setAttn(a);
      })
      .catch((e) => { if (!cancelled) setError(String(e.message ?? e)); })
      .finally(() => { if (!cancelled) setLoading(false); });

    return () => { cancelled = true; };
  }, [id]);

  return (
    <div className="max-w-[1280px] mx-auto px-6 py-6 space-y-5">
      {error && (
        <div className="panel rounded-lg p-4 border-booking/40 text-booking">
          <div className="kicker text-booking">Could not load player</div>
          <div className="mt-1 text-sm">{error}</div>
        </div>
      )}

      {loading && !rating && <HeroSkeleton />}

      {rating && (
        <>
          <PlayerHero p={rating} />
          <StatGrid p={rating} />

          <div className="grid grid-cols-1 lg:grid-cols-[1fr_1.2fr] gap-5">
            <PitchZone topZone={rating.top_pressure_zone} />
            <AttentionHeatmap attn={attn} loading={loading} />
          </div>
        </>
      )}
    </div>
  );
}

function HeroSkeleton() {
  return (
    <div className="panel rounded-lg p-6 animate-pulse">
      <div className="flex gap-6">
        <div className="w-20 h-20 rounded-full bg-ink-800" />
        <div className="flex-1 space-y-3">
          <div className="h-3 w-24 bg-ink-800 rounded" />
          <div className="h-8 w-72 bg-ink-800 rounded" />
          <div className="h-3 w-48 bg-ink-800 rounded" />
        </div>
      </div>
    </div>
  );
}
