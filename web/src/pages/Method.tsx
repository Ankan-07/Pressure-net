const FEATURES: { i: number; name: string; what: string }[] = [
  { i: 0,  name: 'dist_to_nearest_defender', what: 'Euclidean distance to closest opponent' },
  { i: 1,  name: 'dist_to_2nd_defender',     what: 'Distance to 2nd closest opponent' },
  { i: 2,  name: 'dist_to_3rd_defender',     what: 'Distance to 3rd closest opponent' },
  { i: 3,  name: 'closing_speed_nearest',    what: 'Δ nearest-defender distance per second (m/s)' },
  { i: 4,  name: 'closing_speed_2nd',        what: 'Δ 2nd-defender distance per second' },
  { i: 5,  name: 'ball_carrier_x',           what: 'Carrier x, normalised 0–1' },
  { i: 6,  name: 'ball_carrier_y',           what: 'Carrier y, normalised 0–1' },
  { i: 7,  name: 'voronoi_area',             what: 'Voronoi cell area around the ball-carrier' },
  { i: 8,  name: 'pitch_zone',               what: 'Integer 0–5 encoding the pitch region' },
  { i: 9,  name: 'time_since_last_touch',    what: 'Seconds since the previous event' },
  { i: 10, name: 'n_defenders_within_5m',    what: 'Count of opponents within 5m' },
  { i: 11, name: 'pass_lane_density',        what: 'Top-3 angle-binned opponent density' },
  { i: 12, name: 'action_type',              what: 'Pass=0 · Dribble=1 · Carry=2 · Shot=3 · Cross=4' },
  { i: 13, name: 'body_orientation_sin',     what: 'sin(orientation in radians)' },
  { i: 14, name: 'body_orientation_cos',     what: 'cos(orientation in radians)' },
];

export default function MethodPage() {
  return (
    <div className="max-w-[1000px] mx-auto px-6 py-6 space-y-8">
      <Hero />
      <Section kicker="Definition" title="WHAT THE NUMBER MEANS">
        <p>
          <span className="text-chalk">PAP</span> — Pressure-Adjusted Performance —
          is the gap between a player's <span className="text-chalk">observed success rate</span>{' '}
          under pressure and the success rate a calibrated Transformer predicts for an
          <span className="text-chalk"> average player in the same pressing situation</span>.
        </p>
        <p>
          A PAP of <span className="stat-num text-pitch">+0.087</span> means the player
          completes pressing actions 8.7 percentage points more often than the
          baseline expectation. Negative PAP means the opposite.
        </p>
        <Formula />
      </Section>

      <Section kicker="Pipeline" title="HOW IT'S COMPUTED">
        <ol className="list-decimal pl-5 space-y-3 marker:text-pitch marker:font-semibold">
          <li>
            <span className="text-chalk">Filter to pressing events</span> from StatsBomb
            360 data — passes, carries, dribbles, shots flagged{' '}
            <code className="px-1 py-px rounded bg-ink-800 text-chalk text-[12px]">under_pressure</code>{' '}
            with a freeze frame, ball-carrier ≠ keeper.
          </li>
          <li>
            <span className="text-chalk">Engineer 15 features</span> per timestep
            from the freeze-frame snapshot (defender distances, closing speeds,
            Voronoi area, pitch zone, lane density, etc.).
          </li>
          <li>
            <span className="text-chalk">Stack 3 timesteps</span> (t−2, t−1, t=0)
            into a (3 × 15) tensor — the temporal window of how the pressure
            developed.
          </li>
          <li>
            <span className="text-chalk">Score with a Transformer encoder</span>{' '}
            (d=64, 2 layers, 4 heads, CLS token) trained on a 70/15/15
            match-id split, then <span className="text-chalk">Platt-scale</span>{' '}
            the logits into calibrated success probabilities.
          </li>
          <li>
            <span className="text-chalk">Aggregate per player</span>:{' '}
            <code className="px-1 py-px rounded bg-ink-800 text-chalk text-[12px]">PAP = success_rate − mean(p_cal)</code>{' '}
            over all that player's pressing events; report a percentile within the qualifying cohort.
          </li>
        </ol>
      </Section>

      <Section kicker="Architecture" title="WHY THESE CHOICES">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Choice
            head="Match-id split"
            body="Splitting randomly by event row leaks match context — model learns the game, not generalisable pressure patterns. We split 70/15/15 by match_id (seed=42)."
          />
          <Choice
            head="Calibration via Platt"
            body="Raw logits are not probabilities. Without calibration the PAP score is a ranking only. Platt scaling fits a 1-D logistic regression on held-out validation logits."
          />
          <Choice
            head="Transformer, not XGBoost"
            body="XGBoost ties on AUC at T=3 but can't produce per-timestep attention. We keep the Transformer specifically for the (3 × 15) attention heatmaps that drive explainability."
          />
          <Choice
            head="d_model = 64"
            body="With ~50k training events, d_model=128 overfits immediately. d=64 is the right capacity trade-off — empirically tied AUC at half the parameters."
          />
        </div>
      </Section>

      <Section kicker="Feature spec" title="THE 15 INPUTS">
        <div className="panel rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-ink-300 bg-ink-850/60">
                <th className="px-4 py-2 text-left text-[11px] uppercase tracking-wider2 w-12">i</th>
                <th className="px-4 py-2 text-left text-[11px] uppercase tracking-wider2">Feature</th>
                <th className="px-4 py-2 text-left text-[11px] uppercase tracking-wider2">Computation</th>
              </tr>
            </thead>
            <tbody>
              {FEATURES.map((f) => (
                <tr key={f.i} className="border-t hairline">
                  <td className="px-4 py-2 stat-num text-ink-400">{f.i}</td>
                  <td className="px-4 py-2 stat-num text-chalk">{f.name}</td>
                  <td className="px-4 py-2 text-ink-200">{f.what}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Section>

      <Section kicker="Numbers we care about" title="HEADLINE METRICS">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <Metric kicker="Test AUC (Transformer)" value="0.845" />
          <Metric kicker="Test AUC (XGBoost)"     value="0.847" />
          <Metric kicker="Training events"        value="41,606" />
          <Metric kicker="Qualifying players"     value="≥ 100 events" />
        </div>
      </Section>

      <Section kicker="Provenance" title="DATA · MODEL · CODE">
        <ul className="space-y-2 text-sm text-ink-200">
          <li>· Data: <span className="text-chalk">StatsBomb Open Data</span> — EURO 2024, EURO 2020, World Cup 2022.</li>
          <li>· Three competitions, ~59,719 pressing events filtered from ~615k total.</li>
          <li>· Trained models: Logistic Regression · XGBoost · bi-LSTM · Transformer.</li>
          <li>· This dashboard serves the Transformer head; XGBoost and LSTM are kept as baselines.</li>
        </ul>
      </Section>
    </div>
  );
}

function Hero() {
  return (
    <div className="pt-2 panel rounded-lg p-6 relative overflow-hidden">
      <div className="absolute inset-y-0 left-0 w-[3px] bg-pitch" />
      <div className="kicker">Methodology</div>
      <h1 className="font-display tracking-wider2 text-chalk text-4xl leading-none mt-1">
        HOW PRESSURENET SCORES PRESSING
      </h1>
      <p className="mt-3 max-w-2xl text-ink-200 text-sm leading-relaxed">
        The short version: take freeze-frame snapshots of pressing situations,
        let a Transformer guess how a typical player would handle them, then
        score every player by how much they beat that expectation.
      </p>
    </div>
  );
}

function Section({
  kicker,
  title,
  children,
}: {
  kicker: string;
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section>
      <div className="kicker">{kicker}</div>
      <h2 className="font-display tracking-wider2 text-chalk text-2xl leading-none mt-1 mb-4">
        {title}
      </h2>
      <div className="text-ink-200 leading-relaxed space-y-3 text-[15px]">{children}</div>
    </section>
  );
}

function Formula() {
  return (
    <div className="panel rounded-lg p-4 mt-3">
      <div className="kicker mb-2">Definition</div>
      <code className="block stat-num text-pitch text-lg">
        PAP<sub className="text-ink-300">p</sub> ={' '}
        <span className="text-chalk">success_rate</span><sub className="text-ink-300">p</sub>{' '}
        − mean<sub className="text-ink-300">e ∈ p</sub>{' '}
        <span className="text-chalk">p_cal(e)</span>
      </code>
      <p className="mt-2 text-xs text-ink-300">
        Average over a player's pressing events <em>e</em>. p_cal is the
        Platt-scaled Transformer probability for the event.
      </p>
    </div>
  );
}

function Choice({ head, body }: { head: string; body: string }) {
  return (
    <div className="panel rounded-lg p-4">
      <div className="text-chalk font-medium">{head}</div>
      <p className="mt-1.5 text-sm text-ink-300">{body}</p>
    </div>
  );
}

function Metric({ kicker, value }: { kicker: string; value: string }) {
  return (
    <div className="panel rounded-lg p-4">
      <div className="kicker">{kicker}</div>
      <div className="mt-1 stat-num text-2xl text-chalk">{value}</div>
    </div>
  );
}
