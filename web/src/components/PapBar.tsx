type Props = { value: number; max: number };

export function PapBar({ value, max }: Props) {
  const pct = Math.min(100, Math.max(0, (Math.abs(value) / max) * 100));
  const positive = value >= 0;
  return (
    <div className="relative h-1.5 w-24 bg-ink-800 rounded-full overflow-hidden">
      <div
        className={[
          'absolute top-0 bottom-0',
          positive
            ? 'left-1/2 bg-pitch shadow-[0_0_8px_rgba(124,255,107,0.6)]'
            : 'right-1/2 bg-booking',
        ].join(' ')}
        style={{ width: `${pct / 2}%` }}
      />
      <div className="absolute left-1/2 top-0 bottom-0 w-px bg-ink-600" />
    </div>
  );
}
