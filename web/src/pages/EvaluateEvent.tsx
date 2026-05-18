import { useState } from 'react';
import { ScenarioBuilder } from '../components/ScenarioBuilder';
import { DifficultyGauge } from '../components/DifficultyGauge';
import { AttentionInline } from '../components/AttentionInline';
import { AttentionHeatmap } from '../components/AttentionHeatmap';
import { PitchZone } from '../components/PitchZone';
import { evaluateEvent } from '../lib/api';
import { DEFAULT_SCENARIO, scenarioToFeatures, type Scenario } from '../lib/scenario';
import type { EvaluateEventResponse } from '../lib/types';

export default function EvaluateEventPage() {
  const [scenario, setScenario] = useState<Scenario>(DEFAULT_SCENARIO);
  const [result, setResult] = useState<EvaluateEventResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function run() {
    setLoading(true);
    setError(null);
    try {
      const features = scenarioToFeatures(scenario);
      const r = await evaluateEvent(features);
      setResult(r);
    } catch (e) {
      setError(String((e as Error).message ?? e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="max-w-[1280px] mx-auto px-6 py-6 space-y-5">
      <div className="pt-2">
        <div className="kicker">Event evaluator</div>
        <h2 className="mt-1 font-display text-4xl tracking-wider2 text-chalk leading-none">
          HOW HARD IS THIS SITUATION?
        </h2>
        <p className="mt-2 text-sm text-ink-300 max-w-2xl">
          Construct a pressing situation and the Transformer scores it against
          everything it saw in training — returning calibrated difficulty,
          training-set percentile, and what features it actually attended to.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-[1.3fr_1fr] gap-5">
        <ScenarioBuilder value={scenario} onChange={setScenario} onSubmit={run} loading={loading} />
        <PitchZone topZone={scenario.zone} />
      </div>

      {error && (
        <div className="panel rounded-lg p-4 border-booking/40 text-booking">
          <div className="kicker text-booking">API error</div>
          <div className="mt-1 text-sm">{error}</div>
        </div>
      )}

      {result && (
        <>
          <div className="grid grid-cols-1 lg:grid-cols-[1fr_1.4fr] gap-5">
            <DifficultyGauge difficulty={result.difficulty_score} percentile={result.percentile} />
            <Interpretation result={result} />
          </div>
          <AttentionHeatmap
            attn={{
              player_id: 0,
              timesteps: result.timesteps,
              feature_names: result.feature_names,
              heatmap: result.heatmap,
            }}
            loading={false}
          />
          <AttentionInline attn={result.attention_weights} />
        </>
      )}

      {!result && !error && (
        <div className="panel rounded-lg p-8 text-center text-ink-300">
          <div className="kicker">Awaiting input</div>
          <div className="mt-2 text-sm">
            Pick a preset or move the sliders, then hit <span className="text-pitch font-medium">Evaluate</span>.
          </div>
        </div>
      )}
    </div>
  );
}

function Interpretation({ result }: { result: EvaluateEventResponse }) {
  return (
    <div className="panel rounded-lg p-5 flex flex-col">
      <div className="kicker">Interpretation</div>
      <p className="mt-2 text-chalk text-lg leading-snug">{result.interpretation}</p>
      <div className="mt-auto pt-4 flex items-center gap-3 text-xs text-ink-300 border-t hairline mt-4">
        <span className="pill-pitch">success p = {(result.success_probability * 100).toFixed(1)}%</span>
        <span className="pill-ink stat-num">difficulty = {result.difficulty_score.toFixed(3)}</span>
        <span className="pill-whistle">pctl {result.percentile.toFixed(1)}</span>
      </div>
    </div>
  );
}
