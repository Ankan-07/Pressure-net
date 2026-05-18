export type LeaderboardRow = {
  rank: number;
  player_id: number;
  player_name: string;
  team: string;
  pap_score: number;
  pap_percentile: number;
  n_pressing_events: number;
};

export type LeaderboardResponse = {
  leaderboard: LeaderboardRow[];
  total_qualifying_players: number;
  competition: string | null;
  season: string | null;
};

export type SearchMatch = {
  player_id: number;
  player_name: string;
  team: string;
  season: string;
  pap_score: number;
  pap_percentile: number;
};

export type SearchResponse = {
  query: string;
  matches: SearchMatch[];
};

export type HealthResponse = {
  status: string;
  model_version: string;
  calibrated: boolean;
  n_training_events: number;
};

export type PlayerRating = {
  player_id: number;
  player_name: string;
  team: string;
  competition: string;
  season: string;
  pap_score: number;
  pap_percentile: number;
  n_pressing_events: number;
  success_rate: number;
  expected_success: number;
  avg_difficulty_faced: number;
  top_pressure_zone: string;
};

export type PlayerAttention = {
  player_id: number;
  timesteps: string[];
  feature_names: string[];
  heatmap: number[][];
};

export type EvaluateEventResponse = {
  success_probability: number;
  difficulty_score: number;
  percentile: number;
  interpretation: string;
  attention_weights: {
    t0: Record<string, number>;
    't-1': Record<string, number>;
    't-2': Record<string, number>;
  };
  timesteps: string[];
  feature_names: string[];
  heatmap: number[][];
};

export type Competition = {
  key: string;
  label: string;
  competition_id: number;
  season_id: number;
};

export const COMPETITIONS: Competition[] = [
  { key: 'all',      label: 'All tournaments', competition_id: 0, season_id: 0 },
  { key: 'euro2024', label: 'UEFA Euro 2024',  competition_id: 55, season_id: 282 },
  { key: 'euro2020', label: 'UEFA Euro 2020',  competition_id: 55, season_id: 43  },
  { key: 'wc2022',   label: 'World Cup 2022',  competition_id: 43, season_id: 106 },
];
