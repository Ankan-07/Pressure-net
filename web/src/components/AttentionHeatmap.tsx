import { Fragment } from 'react';
import type { PlayerAttention } from '../lib/types';

type Props = { attn: PlayerAttention | null; loading: boolean };

const FEATURE_LABELS: Record<string, string> = {
  dist_to_nearest_defender: 'D1 dist',
  dist_to_2nd_defender: 'D2 dist',
  dist_to_3rd_defender: 'D3 dist',
  closing_speed_nearest: 'D1 close',
  closing_speed_2nd: 'D2 close',
  ball_carrier_x: 'BC x',
  ball_carrier_y: 'BC y',
  voronoi_area: 'Voronoi',
  pitch_zone: 'Zone',
  time_since_last_touch: 'Δt touch',
  n_defenders_within_5m: 'D ≤5m',
  pass_lane_density: 'Lane dens',
  action_type: 'Action',
  body_orientation_sin: 'Orient sin',
  body_orientation_cos: 'Orient cos',
};

export function AttentionHeatmap({ attn, loading }: Props) {
  return (
    <div className="panel rounded-lg p-4">
      <div className="flex items-center mb-3">
        <div>
          <div className="kicker">Attention profile</div>
          <h3 className="font-display tracking-wider2 text-chalk text-lg leading-none mt-1">
            WHAT THE MODEL LOOKED AT
          </h3>
        </div>
        <span className="ml-auto pill-ink">3 × 15</span>
      </div>

      {loading || !attn ? (
        <Skeleton />
      ) : (
        <Heatmap attn={attn} />
      )}

      <p className="mt-4 text-xs text-ink-300 leading-relaxed">
        Each cell is the model's averaged attention from the CLS token to a
        (timestep, feature) pair across this player's pressing events,
        multiplied by the feature's standardized magnitude. Brighter cells = the
        signal the model leans on hardest when judging this player.
      </p>
    </div>
  );
}

function Heatmap({ attn }: { attn: PlayerAttention }) {
  const flat = attn.heatmap.flat();
  const absMax = Math.max(1e-9, ...flat.map((v) => Math.abs(v)));

  const intensity = (v: number) => Math.min(1, Math.abs(v) / absMax);

  return (
    <div className="overflow-x-auto">
      <div className="inline-block min-w-full">
        <div
          className="grid gap-0.5"
          style={{
            gridTemplateColumns: `auto repeat(${attn.feature_names.length}, minmax(36px, 1fr))`,
          }}
        >
          <div />
          {attn.feature_names.map((name) => (
            <div
              key={name}
              className="text-[9px] uppercase tracking-wider2 text-ink-400 text-center pb-1.5 leading-tight"
              title={name}
            >
              {FEATURE_LABELS[name] ?? name}
            </div>
          ))}

          {attn.timesteps.map((ts, row) => (
            <RowGroup
              key={ts}
              label={ts}
              row={attn.heatmap[row]}
              intensity={intensity}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

function RowGroup({
  label,
  row,
  intensity,
}: {
  label: string;
  row: number[];
  intensity: (v: number) => number;
}) {
  return (
    <>
      <div className="text-[11px] uppercase tracking-wider2 text-ink-300 pr-2 self-center stat-num">
        {label}
      </div>
      {row.map((v, i) => {
        const a = intensity(v);
        return (
          <div
            key={i}
            title={`${label} · ${v.toFixed(4)}`}
            className="h-9 rounded-[3px] border border-ink-800 flex items-center justify-center"
            style={{
              backgroundColor: `rgba(124,255,107,${(a * 0.85).toFixed(3)})`,
              boxShadow: a > 0.75 ? `0 0 8px rgba(124,255,107,${(a * 0.4).toFixed(3)})` : undefined,
            }}
          >
            {a > 0.65 && (
              <span className="text-[9px] text-ink-950 font-semibold stat-num">
                {(a * 100).toFixed(0)}
              </span>
            )}
          </div>
        );
      })}
    </>
  );
}

function Skeleton() {
  return (
    <div className="grid gap-0.5" style={{ gridTemplateColumns: 'auto repeat(15, minmax(36px, 1fr))' }}>
      <div />
      {Array.from({ length: 15 }).map((_, i) => (
        <div key={i} className="h-3 rounded bg-ink-800 mb-1 animate-pulse" />
      ))}
      {Array.from({ length: 3 }).map((_, r) => (
        <Fragment key={r}>
          <div className="h-9 rounded bg-ink-800 animate-pulse" />
          {Array.from({ length: 15 }).map((_, c) => (
            <div key={c} className="h-9 rounded bg-ink-800 animate-pulse" />
          ))}
        </Fragment>
      ))}
    </div>
  );
}
