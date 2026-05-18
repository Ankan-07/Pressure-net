export type ActionType = 'pass' | 'dribble' | 'carry' | 'shot' | 'cross';
export type ZoneKey =
  | 'own_box'
  | 'def_third'
  | 'left_half_space'
  | 'right_half_space'
  | 'final_third'
  | 'opp_box';

export const ACTION_TYPES: { key: ActionType; label: string; code: number }[] = [
  { key: 'pass',    label: 'Pass',    code: 0 },
  { key: 'dribble', label: 'Dribble', code: 1 },
  { key: 'carry',   label: 'Carry',   code: 2 },
  { key: 'shot',    label: 'Shot',    code: 3 },
  { key: 'cross',   label: 'Cross',   code: 4 },
];

export const ZONES: { key: ZoneKey; label: string; code: number; x: number; y: number }[] = [
  { key: 'own_box',          label: 'Own box',          code: 0, x: 0.08, y: 0.50 },
  { key: 'def_third',        label: 'Def third',        code: 1, x: 0.25, y: 0.50 },
  { key: 'left_half_space',  label: 'Left half-space',  code: 2, x: 0.50, y: 0.25 },
  { key: 'right_half_space', label: 'Right half-space', code: 3, x: 0.50, y: 0.75 },
  { key: 'final_third',      label: 'Final third',      code: 4, x: 0.75, y: 0.50 },
  { key: 'opp_box',          label: 'Opp box',          code: 5, x: 0.92, y: 0.50 },
];

export type Scenario = {
  action: ActionType;
  zone: ZoneKey;
  distNearest: number;      // metres
  closingSpeed: number;     // m/s
  nDefendersWithin5m: number;
  bodyOrientationDeg: number;
};

export const DEFAULT_SCENARIO: Scenario = {
  action: 'pass',
  zone: 'final_third',
  distNearest: 2.5,
  closingSpeed: 1.5,
  nDefendersWithin5m: 3,
  bodyOrientationDeg: 0,
};

export type Preset = { key: string; label: string; sub: string; scenario: Scenario };

export const PRESETS: Preset[] = [
  {
    key: 'open_midfield',
    label: 'Open midfield carry',
    sub: 'Lots of space, slow press',
    scenario: { action: 'carry', zone: 'left_half_space', distNearest: 6.0, closingSpeed: 0.3, nDefendersWithin5m: 1, bodyOrientationDeg: 0 },
  },
  {
    key: 'tight_final_third',
    label: 'Tight final third pass',
    sub: 'Two defenders inside 3m',
    scenario: { action: 'pass', zone: 'final_third', distNearest: 1.8, closingSpeed: 2.1, nDefendersWithin5m: 3, bodyOrientationDeg: 0 },
  },
  {
    key: 'box_dribble',
    label: 'Penalty box dribble',
    sub: 'Defender closing hard',
    scenario: { action: 'dribble', zone: 'opp_box', distNearest: 1.2, closingSpeed: 3.4, nDefendersWithin5m: 4, bodyOrientationDeg: 0 },
  },
  {
    key: 'wide_cross',
    label: 'Wide cross under pressure',
    sub: 'Pinned to touchline',
    scenario: { action: 'cross', zone: 'right_half_space', distNearest: 2.0, closingSpeed: 2.0, nDefendersWithin5m: 2, bodyOrientationDeg: 90 },
  },
];

export function scenarioToFeatures(s: Scenario): number[][] {
  const zone = ZONES.find((z) => z.key === s.zone)!;
  const action = ACTION_TYPES.find((a) => a.key === s.action)!;

  const d1 = s.distNearest;
  const d2 = d1 + 1.2;
  const d3 = d1 + 2.4;
  const closing1 = s.closingSpeed;
  const closing2 = Math.max(0, s.closingSpeed - 0.5);
  const bcX = zone.x;
  const bcY = zone.y;
  const voronoi = clamp(40 - 10 * s.nDefendersWithin5m - 1.5 * (5 - Math.min(5, d1)), 2, 60);
  const zoneCode = zone.code;
  const dtTouch = 0.6;
  const nDef5 = s.nDefendersWithin5m;
  const laneDensity = clamp(0.15 + 0.06 * s.nDefendersWithin5m - 0.02 * d1, 0, 1);
  const actionCode = action.code;
  const orientRad = (s.bodyOrientationDeg * Math.PI) / 180;
  const orientSin = Math.sin(orientRad);
  const orientCos = Math.cos(orientRad);

  const t0 = [d1, d2, d3, closing1, closing2, bcX, bcY, voronoi, zoneCode, dtTouch, nDef5, laneDensity, actionCode, orientSin, orientCos];

  // t-1: defenders slightly further, closing speed lower (pressure was building)
  const t1 = [
    d1 + 0.8, d2 + 0.8, d3 + 0.8,
    Math.max(0, closing1 - 0.4), Math.max(0, closing2 - 0.4),
    bcX, bcY, voronoi + 4, zoneCode, dtTouch, Math.max(0, nDef5 - 1),
    Math.max(0, laneDensity - 0.05), actionCode, orientSin, orientCos,
  ];

  // t-2: even further out
  const t2 = [
    d1 + 1.6, d2 + 1.6, d3 + 1.6,
    Math.max(0, closing1 - 0.8), Math.max(0, closing2 - 0.8),
    bcX, bcY, voronoi + 8, zoneCode, dtTouch, Math.max(0, nDef5 - 2),
    Math.max(0, laneDensity - 0.1), actionCode, orientSin, orientCos,
  ];

  // API expects rows in [t-2, t-1, t=0] order.
  return [t2, t1, t0];
}

function clamp(v: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, v));
}
