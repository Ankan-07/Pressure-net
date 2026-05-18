export function fmtPap(v: number): string {
  const sign = v >= 0 ? '+' : '−';
  return `${sign}${Math.abs(v).toFixed(3)}`;
}

export function fmtPct(v: number, digits = 1): string {
  return `${v.toFixed(digits)}`;
}

export function papTone(v: number): 'pitch' | 'whistle' | 'booking' | 'ink' {
  if (v >= 0.04) return 'pitch';
  if (v >= 0.0)  return 'whistle';
  if (v >= -0.04) return 'ink';
  return 'booking';
}

export function shortTeam(team: string): string {
  if (team.length <= 12) return team;
  return team.slice(0, 11) + '…';
}

export function initials(name: string): string {
  const parts = name.split(' ').filter(Boolean);
  if (parts.length === 0) return '?';
  if (parts.length === 1) return parts[0][0]?.toUpperCase() ?? '?';
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
}
