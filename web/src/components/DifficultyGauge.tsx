type Props = { difficulty: number; percentile: number };

export function DifficultyGauge({ difficulty, percentile }: Props) {
  const pct = Math.max(0, Math.min(1, difficulty));
  const arcLen = 220;
  const filled = arcLen * pct;
  const remaining = arcLen - filled;

  const tone =
    difficulty >= 0.7 ? '#E5484D' :
    difficulty >= 0.5 ? '#FFB627' :
    difficulty >= 0.3 ? '#A3FF96' : '#5BD64C';

  return (
    <div className="panel rounded-lg p-5">
      <div className="kicker">Difficulty</div>
      <div className="flex items-center gap-6 mt-1">
        <div className="relative w-[180px] h-[110px] shrink-0">
          <svg viewBox="0 0 200 120" className="w-full h-full overflow-visible">
            <path
              d="M 20 110 A 80 80 0 0 1 180 110"
              fill="none"
              stroke="#1F2833"
              strokeWidth="14"
              strokeLinecap="round"
            />
            <path
              d="M 20 110 A 80 80 0 0 1 180 110"
              fill="none"
              stroke={tone}
              strokeWidth="14"
              strokeLinecap="round"
              strokeDasharray={`${filled} ${remaining}`}
              style={{ transition: 'stroke-dasharray 400ms ease, stroke 200ms' }}
            />
            <text
              x="100"
              y="95"
              textAnchor="middle"
              className="font-mono"
              style={{ fill: '#F5F5F0', fontSize: 30, fontWeight: 600 }}
            >
              {difficulty.toFixed(2)}
            </text>
            <text
              x="100"
              y="115"
              textAnchor="middle"
              style={{ fill: '#8A95A4', fontSize: 9, letterSpacing: 2, textTransform: 'uppercase' }}
            >
              0 — 1
            </text>
          </svg>
        </div>

        <div className="space-y-2">
          <div>
            <div className="kicker">Percentile</div>
            <div className="stat-num text-2xl text-chalk">{percentile.toFixed(1)}</div>
          </div>
          <div>
            <div className="kicker">Success probability</div>
            <div className="stat-num text-2xl text-pitch">
              {((1 - difficulty) * 100).toFixed(1)}%
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
