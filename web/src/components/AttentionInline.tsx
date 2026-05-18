import type { EvaluateEventResponse } from '../lib/types';

type Props = { attn: EvaluateEventResponse['attention_weights'] };

const PRETTY: Record<string, string> = {
  dist_to_nearest_defender: 'Nearest defender distance',
  dist_to_2nd_defender: '2nd defender distance',
  dist_to_3rd_defender: '3rd defender distance',
  closing_speed_nearest: 'Nearest closing speed',
  closing_speed_2nd: '2nd closing speed',
  ball_carrier_x: 'Carrier x',
  ball_carrier_y: 'Carrier y',
  voronoi_area: 'Voronoi area',
  pitch_zone: 'Pitch zone',
  time_since_last_touch: 'Time since last touch',
  n_defenders_within_5m: 'Defenders within 5m',
  pass_lane_density: 'Pass-lane density',
  action_type: 'Action type',
  body_orientation_sin: 'Body orientation (sin)',
  body_orientation_cos: 'Body orientation (cos)',
};

export function AttentionInline({ attn }: Props) {
  return (
    <div className="panel rounded-lg p-5">
      <div className="kicker">Attention readout</div>
      <h3 className="font-display tracking-wider2 text-chalk text-lg leading-none mt-1">
        WHAT THE MODEL LEANED ON
      </h3>

      <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-4">
        <Col label="t = 0  (moment of action)" data={attn.t0} />
        <Col label="t − 1" data={attn['t-1']} />
        <Col label="t − 2" data={attn['t-2']} />
      </div>
    </div>
  );
}

function Col({ label, data }: { label: string; data: Record<string, number> }) {
  const entries = Object.entries(data).sort((a, b) => b[1] - a[1]);
  const max = Math.max(1e-9, ...entries.map(([, v]) => v));
  return (
    <div className="border-l hairline pl-3">
      <div className="text-[11px] uppercase tracking-wider2 text-ink-300 stat-num mb-2">
        {label}
      </div>
      {entries.length === 0 && (
        <div className="text-xs text-ink-400 italic">No salient features</div>
      )}
      <ul className="space-y-2">
        {entries.map(([name, v]) => {
          const w = Math.min(100, (v / max) * 100);
          return (
            <li key={name}>
              <div className="flex items-baseline justify-between text-xs">
                <span className="text-ink-100 truncate pr-2">{PRETTY[name] ?? name}</span>
                <span className="stat-num text-pitch">{v.toFixed(3)}</span>
              </div>
              <div className="h-1 rounded-full bg-ink-800 mt-1 overflow-hidden">
                <div
                  className="h-full bg-pitch/70"
                  style={{ width: `${w}%` }}
                />
              </div>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
