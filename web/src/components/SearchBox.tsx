import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { searchPlayers } from '../lib/api';
import { fmtPap, initials } from '../lib/format';
import type { SearchMatch } from '../lib/types';

export function SearchBox() {
  const navigate = useNavigate();
  const [q, setQ] = useState('');
  const [matches, setMatches] = useState<SearchMatch[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [activeIdx, setActiveIdx] = useState(0);
  const wrapRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (q.trim().length < 2) {
      setMatches([]);
      return;
    }
    let cancelled = false;
    const handle = setTimeout(() => {
      setLoading(true);
      searchPlayers(q.trim(), 8)
        .then((r) => { if (!cancelled) { setMatches(r.matches); setActiveIdx(0); } })
        .catch(() => { if (!cancelled) setMatches([]); })
        .finally(() => { if (!cancelled) setLoading(false); });
    }, 180);
    return () => { cancelled = true; clearTimeout(handle); };
  }, [q]);

  useEffect(() => {
    function onClickAway(e: MouseEvent) {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener('mousedown', onClickAway);
    return () => document.removeEventListener('mousedown', onClickAway);
  }, []);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        inputRef.current?.focus();
        setOpen(true);
      }
    }
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, []);

  function selectMatch(m: SearchMatch) {
    setOpen(false);
    setQ('');
    setMatches([]);
    navigate(`/players/${m.player_id}`);
  }

  function onKeyDown(e: React.KeyboardEvent) {
    if (!open || matches.length === 0) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActiveIdx((i) => Math.min(matches.length - 1, i + 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActiveIdx((i) => Math.max(0, i - 1));
    } else if (e.key === 'Enter') {
      e.preventDefault();
      const m = matches[activeIdx];
      if (m) selectMatch(m);
    } else if (e.key === 'Escape') {
      setOpen(false);
    }
  }

  const showPanel = open && q.trim().length >= 2;

  return (
    <div ref={wrapRef} className="relative w-64">
      <div className="flex items-center gap-2 bg-ink-800/80 border hairline rounded-md px-2.5 py-1.5
                      focus-within:border-pitch/50 focus-within:bg-ink-800 transition-colors">
        <SearchIcon className="w-3.5 h-3.5 text-ink-400" />
        <input
          ref={inputRef}
          value={q}
          onChange={(e) => { setQ(e.target.value); setOpen(true); }}
          onFocus={() => setOpen(true)}
          onKeyDown={onKeyDown}
          placeholder="Search players…"
          className="bg-transparent outline-none flex-1 text-sm text-chalk placeholder:text-ink-400"
        />
        <kbd className="text-[10px] stat-num text-ink-400 border border-ink-700 rounded px-1 py-px">
          ⌘K
        </kbd>
      </div>

      {showPanel && (
        <div className="absolute top-full left-0 right-0 mt-1.5 panel rounded-md p-1 z-50 max-h-80 overflow-auto shadow-xl">
          {loading && matches.length === 0 && (
            <div className="px-3 py-3 text-xs text-ink-300">Searching…</div>
          )}
          {!loading && matches.length === 0 && (
            <div className="px-3 py-3 text-xs text-ink-300">
              No qualifying players match <span className="text-chalk">"{q}"</span>.
            </div>
          )}
          {matches.map((m, i) => {
            const positive = m.pap_score >= 0;
            return (
              <button
                key={m.player_id}
                onMouseEnter={() => setActiveIdx(i)}
                onClick={() => selectMatch(m)}
                className={[
                  'w-full flex items-center gap-3 px-2.5 py-2 rounded text-left transition-colors',
                  i === activeIdx ? 'bg-ink-800' : 'hover:bg-ink-800/60',
                ].join(' ')}
              >
                <div className="w-7 h-7 rounded-full bg-ink-850 border hairline flex items-center justify-center text-[10px] font-semibold text-ink-200 shrink-0">
                  {initials(m.player_name)}
                </div>
                <div className="min-w-0 flex-1">
                  <div className="text-sm text-chalk truncate">{m.player_name}</div>
                  <div className="text-[11px] text-ink-300 truncate">
                    {m.team} · {m.season}
                  </div>
                </div>
                <div className={[
                  'text-xs stat-num font-medium shrink-0',
                  positive ? 'text-pitch' : 'text-booking',
                ].join(' ')}>
                  {fmtPap(m.pap_score)}
                </div>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

function SearchIcon({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
         strokeLinecap="round" strokeLinejoin="round" className={className}>
      <circle cx="11" cy="11" r="7" />
      <path d="M20 20l-3.5-3.5" />
    </svg>
  );
}
