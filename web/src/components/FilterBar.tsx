import { COMPETITIONS, type Competition } from '../lib/types';

type Props = {
  selected: Competition;
  onSelect: (c: Competition) => void;
  minEvents: number;
  onMinEvents: (v: number) => void;
  topN: number;
  onTopN: (v: number) => void;
};

export function FilterBar({
  selected,
  onSelect,
  minEvents,
  onMinEvents,
  topN,
  onTopN,
}: Props) {
  return (
    <div className="panel rounded-lg p-3 flex flex-wrap items-center gap-3">
      <div className="flex items-center gap-1.5">
        <span className="kicker mr-1">Competition</span>
        {COMPETITIONS.map((c) => {
          const active = c.key === selected.key;
          return (
            <button
              key={c.key}
              onClick={() => onSelect(c)}
              className={[
                'px-3 py-1.5 rounded-md text-xs font-medium border transition-colors',
                active
                  ? 'bg-pitch/10 border-pitch/40 text-pitch'
                  : 'bg-ink-800/60 border-ink-700 text-ink-200 hover:text-chalk hover:border-ink-500',
              ].join(' ')}
            >
              {c.label}
            </button>
          );
        })}
      </div>

      <div className="ml-auto flex items-center gap-4">
        <NumField
          label="Min events"
          value={minEvents}
          onChange={onMinEvents}
          min={1}
          step={10}
        />
        <NumField
          label="Top N"
          value={topN}
          onChange={onTopN}
          min={1}
          max={500}
          step={10}
        />
      </div>
    </div>
  );
}

function NumField({
  label,
  value,
  onChange,
  min,
  max,
  step,
}: {
  label: string;
  value: number;
  onChange: (v: number) => void;
  min?: number;
  max?: number;
  step?: number;
}) {
  return (
    <label className="flex items-center gap-2">
      <span className="kicker">{label}</span>
      <input
        type="number"
        value={value}
        min={min}
        max={max}
        step={step}
        onChange={(e) => onChange(Number(e.target.value) || 0)}
        className="w-20 bg-ink-800 border border-ink-700 rounded-md px-2 py-1 text-sm text-chalk
                   stat-num focus:outline-none focus:border-pitch/60 focus:bg-ink-850"
      />
    </label>
  );
}
