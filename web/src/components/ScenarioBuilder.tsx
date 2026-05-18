import { ACTION_TYPES, PRESETS, ZONES, type Scenario } from '../lib/scenario';

type Props = {
  value: Scenario;
  onChange: (s: Scenario) => void;
  onSubmit: () => void;
  loading: boolean;
};

export function ScenarioBuilder({ value, onChange, onSubmit, loading }: Props) {
  const set = <K extends keyof Scenario>(k: K, v: Scenario[K]) =>
    onChange({ ...value, [k]: v });

  return (
    <div className="panel rounded-lg p-5 space-y-5">
      <div className="flex items-center">
        <div>
          <div className="kicker">Scenario</div>
          <h3 className="font-display tracking-wider2 text-chalk text-lg leading-none mt-1">
            BUILD A PRESSING SITUATION
          </h3>
        </div>
        <button
          onClick={onSubmit}
          disabled={loading}
          className="ml-auto px-4 py-2 rounded-md text-sm font-semibold bg-pitch text-ink-950
                     hover:bg-pitch-glow disabled:opacity-50 disabled:cursor-not-allowed
                     shadow-[0_0_18px_rgba(124,255,107,0.35)] transition"
        >
          {loading ? 'Scoring…' : 'Evaluate'}
        </button>
      </div>

      <div>
        <div className="kicker mb-2">Presets</div>
        <div className="flex flex-wrap gap-2">
          {PRESETS.map((p) => (
            <button
              key={p.key}
              onClick={() => onChange(p.scenario)}
              className="text-left px-3 py-2 rounded-md border hairline bg-ink-800/60
                         hover:border-pitch/40 hover:bg-ink-800 transition"
            >
              <div className="text-sm text-chalk">{p.label}</div>
              <div className="text-[11px] text-ink-300">{p.sub}</div>
            </button>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
        <Group label="Action">
          <Chips
            value={value.action}
            options={ACTION_TYPES.map((a) => ({ key: a.key, label: a.label }))}
            onChange={(v) => set('action', v)}
          />
        </Group>
        <Group label="Pitch zone">
          <Chips
            value={value.zone}
            options={ZONES.map((z) => ({ key: z.key, label: z.label }))}
            onChange={(v) => set('zone', v)}
          />
        </Group>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
        <Slider
          label="Nearest defender"
          unit="m"
          value={value.distNearest}
          min={0.5}
          max={10}
          step={0.1}
          onChange={(v) => set('distNearest', v)}
        />
        <Slider
          label="Closing speed"
          unit="m/s"
          value={value.closingSpeed}
          min={0}
          max={5}
          step={0.1}
          onChange={(v) => set('closingSpeed', v)}
        />
        <Slider
          label="Defenders ≤ 5m"
          unit=""
          value={value.nDefendersWithin5m}
          min={0}
          max={6}
          step={1}
          onChange={(v) => set('nDefendersWithin5m', v)}
        />
      </div>
    </div>
  );
}

function Group({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="kicker mb-2">{label}</div>
      {children}
    </div>
  );
}

function Chips<K extends string>({
  value,
  options,
  onChange,
}: {
  value: K;
  options: { key: K; label: string }[];
  onChange: (v: K) => void;
}) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {options.map((o) => {
        const active = o.key === value;
        return (
          <button
            key={o.key}
            onClick={() => onChange(o.key)}
            className={[
              'px-2.5 py-1 rounded-md text-xs font-medium border transition-colors',
              active
                ? 'bg-pitch/10 border-pitch/40 text-pitch'
                : 'bg-ink-800/60 border-ink-700 text-ink-200 hover:border-ink-500',
            ].join(' ')}
          >
            {o.label}
          </button>
        );
      })}
    </div>
  );
}

function Slider({
  label,
  unit,
  value,
  min,
  max,
  step,
  onChange,
}: {
  label: string;
  unit: string;
  value: number;
  min: number;
  max: number;
  step: number;
  onChange: (v: number) => void;
}) {
  return (
    <div>
      <div className="flex items-baseline justify-between mb-1.5">
        <span className="kicker">{label}</span>
        <span className="stat-num text-chalk text-sm">
          {step >= 1 ? value.toFixed(0) : value.toFixed(1)}{unit && <span className="text-ink-400 ml-1">{unit}</span>}
        </span>
      </div>
      <input
        type="range"
        min={min}
        max={max}
        step={step}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="w-full accent-pitch h-1 cursor-pointer"
      />
      <div className="flex justify-between text-[10px] text-ink-400 stat-num mt-0.5">
        <span>{min}</span>
        <span>{max}</span>
      </div>
    </div>
  );
}
