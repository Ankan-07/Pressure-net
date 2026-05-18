type Props = { topZone: string; className?: string };

const ZONE_KEYS = [
  'own_box',
  'def_third',
  'left_half_space',
  'right_half_space',
  'final_third',
  'opp_box',
] as const;

const ZONE_LABEL: Record<string, string> = {
  own_box: 'Own box',
  def_third: 'Def third',
  left_half_space: 'Left half-space',
  right_half_space: 'Right half-space',
  final_third: 'Final third',
  opp_box: 'Opp box',
};

export function PitchZone({ topZone, className = '' }: Props) {
  const active = (k: string) => topZone === k;
  const cell = (k: string, extra = '') =>
    [
      'absolute border border-pitch/15 rounded-sm transition-colors flex items-end justify-start p-1.5',
      active(k)
        ? 'bg-pitch/25 border-pitch/60 shadow-[inset_0_0_30px_rgba(124,255,107,0.15)]'
        : 'bg-ink-900/40',
      extra,
    ].join(' ');

  return (
    <div className={`panel rounded-lg p-4 ${className}`}>
      <div className="kicker mb-3">Top pressure zone</div>

      <div className="relative w-full" style={{ aspectRatio: '120 / 80' }}>
        <div className="absolute inset-0 rounded-md overflow-hidden bg-[#0E1A12]"
             style={{
               backgroundImage:
                 'repeating-linear-gradient(90deg, rgba(124,255,107,0.04) 0 8.33%, rgba(124,255,107,0.07) 8.33% 16.66%)',
             }}>
          <div className="absolute inset-0 border-2 border-pitch/30 rounded-md" />
          <div className="absolute top-0 bottom-0 left-1/2 w-px bg-pitch/30" />
          <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-16 h-16 rounded-full border border-pitch/30" />
          <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-1.5 h-1.5 rounded-full bg-pitch/50" />
          <div className="absolute left-0 top-1/2 -translate-y-1/2 w-[18%] h-[60%] border-2 border-l-0 border-pitch/30 rounded-r-md" />
          <div className="absolute right-0 top-1/2 -translate-y-1/2 w-[18%] h-[60%] border-2 border-r-0 border-pitch/30 rounded-l-md" />

          <div className={cell('own_box',         'left-0           top-1/2 -translate-y-1/2 w-[18%] h-[60%]')}>
            <ZoneLabel active={active('own_box')}         label="Own box" />
          </div>
          <div className={cell('def_third',       'left-[18%]       top-0   w-[15%]  h-full')}>
            <ZoneLabel active={active('def_third')}       label="Def third" />
          </div>
          <div className={cell('left_half_space','left-[33%]       top-0   w-[34%]  h-1/2')}>
            <ZoneLabel active={active('left_half_space')} label="Left half-space" />
          </div>
          <div className={cell('right_half_space','left-[33%]      top-1/2 w-[34%]  h-1/2')}>
            <ZoneLabel active={active('right_half_space')} label="Right half-space" />
          </div>
          <div className={cell('final_third',     'right-[18%]     top-0   w-[15%]  h-full')}>
            <ZoneLabel active={active('final_third')}     label="Final third" />
          </div>
          <div className={cell('opp_box',         'right-0          top-1/2 -translate-y-1/2 w-[18%] h-[60%]')}>
            <ZoneLabel active={active('opp_box')}         label="Opp box" />
          </div>
        </div>
      </div>

      <div className="mt-3 flex items-center justify-between text-[10px] uppercase tracking-wider2 text-ink-400">
        <span>← own goal</span>
        <span className="text-ink-200">{ZONE_LABEL[topZone] ?? topZone}</span>
        <span>attacking →</span>
      </div>
      <div className="sr-only">{ZONE_KEYS.join(' ')}</div>
    </div>
  );
}

function ZoneLabel({ active, label }: { active: boolean; label: string }) {
  return (
    <span
      className={[
        'text-[9px] uppercase tracking-wider2 leading-none',
        active ? 'text-pitch' : 'text-ink-400',
      ].join(' ')}
    >
      {label}
    </span>
  );
}
