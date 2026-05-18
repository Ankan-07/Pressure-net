import { useEffect, useState } from 'react';
import { BrowserRouter, Route, Routes } from 'react-router-dom';
import { Header } from './components/Header';
import LeaderboardPage from './pages/Leaderboard';
import PlayerDetailPage from './pages/PlayerDetail';
import EvaluateEventPage from './pages/EvaluateEvent';
import MethodPage from './pages/Method';
import { getHealth } from './lib/api';
import type { HealthResponse } from './lib/types';

export default function App() {
  const [health, setHealth] = useState<HealthResponse | null>(null);

  useEffect(() => {
    getHealth().then(setHealth).catch(() => setHealth(null));
  }, []);

  return (
    <BrowserRouter>
      <div className="min-h-full flex flex-col">
        <Header health={health} />
        <main className="flex-1">
          <Routes>
            <Route path="/" element={<LeaderboardPage health={health} />} />
            <Route path="/players/:playerId" element={<PlayerDetailPage />} />
            <Route path="/evaluate" element={<EvaluateEventPage />} />
            <Route path="/method" element={<MethodPage />} />
          </Routes>

          <div className="max-w-[1280px] mx-auto px-6 pb-10">
            <Footer />
          </div>
        </main>
      </div>
    </BrowserRouter>
  );
}

function Footer() {
  return (
    <footer className="border-t hairline mt-8 pt-5 text-xs text-ink-300 flex flex-wrap items-center gap-3">
      <span className="kicker">PressureNet</span>
      <span>·</span>
      <span>StatsBomb open data</span>
      <span>·</span>
      <span>Calibrated Transformer (Platt scaled)</span>
      <span className="ml-auto stat-num">v1.0</span>
    </footer>
  );
}
