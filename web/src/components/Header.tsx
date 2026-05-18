import { NavLink } from 'react-router-dom';
import { SearchBox } from './SearchBox';
import type { HealthResponse } from '../lib/types';

type Props = { health: HealthResponse | null };

export function Header({ health }: Props) {
  return (
    <header className="border-b hairline">
      <div className="max-w-[1280px] mx-auto px-6 py-4 flex items-center gap-6">
        <div className="flex items-center gap-3">
          <div className="relative">
            <div className="w-9 h-9 rounded-md bg-ink-800 border hairline flex items-center justify-center">
              <span className="font-display text-pitch text-xl leading-none">P</span>
            </div>
            <span className="absolute -bottom-1 -right-1 w-2.5 h-2.5 rounded-full bg-pitch shadow-[0_0_10px_2px_rgba(124,255,107,0.5)]" />
          </div>
          <div>
            <h1 className="font-display text-2xl tracking-wider2 leading-none text-chalk">
              PRESSURE<span className="text-pitch">·</span>NET
            </h1>
            <p className="text-[11px] tracking-widest2 uppercase text-ink-300 mt-1">
              Football performance under pressure
            </p>
          </div>
        </div>

        <div className="ml-auto flex items-center gap-3">
          <SearchBox />
          <nav className="flex items-center gap-1 text-sm">
            <NavItem to="/" end>Leaderboard</NavItem>
            <NavItem to="/evaluate">Evaluate</NavItem>
            <NavItem to="/method">Method</NavItem>
          </nav>
        </div>

        <div className="flex items-center gap-2 pl-4 ml-2 border-l hairline">
          {health ? (
            <>
              <span className="w-2 h-2 rounded-full bg-pitch animate-pulse" />
              <span className="text-[11px] tracking-wider2 uppercase text-ink-200">
                {health.model_version}
              </span>
            </>
          ) : (
            <>
              <span className="w-2 h-2 rounded-full bg-booking" />
              <span className="text-[11px] tracking-wider2 uppercase text-ink-300">
                offline
              </span>
            </>
          )}
        </div>
      </div>
    </header>
  );
}

function NavItem({
  to,
  end,
  children,
}: {
  to: string;
  end?: boolean;
  children: React.ReactNode;
}) {
  return (
    <NavLink
      to={to}
      end={end}
      className={({ isActive }) =>
        [
          'px-3 py-1.5 rounded-md transition-colors',
          isActive
            ? 'text-chalk bg-ink-800 border hairline'
            : 'text-ink-300 hover:text-ink-100',
        ].join(' ')
      }
    >
      {children}
    </NavLink>
  );
}
