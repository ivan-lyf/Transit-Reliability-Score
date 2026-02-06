/**
 * Environment configuration for mobile app.
 * All values are loaded from environment variables or Expo constants.
 */

import Constants from 'expo-constants';

interface EnvConfig {
  apiUrl: string;
  supabaseUrl: string;
  supabaseAnonKey: string;
  mapboxAccessToken: string;
  environment: 'development' | 'staging' | 'production';
}

function getEnvConfig(): EnvConfig {
  const extra = Constants.expoConfig?.extra ?? {};
  const getString = (key: string): string | undefined => {
    const value = extra[key];
    if (typeof value === 'string' && value.trim().length > 0) {
      return value;
    }
    return undefined;
  };

  return {
    apiUrl: getString('API_URL') ?? 'http://localhost:8000',
    supabaseUrl: getString('SUPABASE_URL') ?? '',
    supabaseAnonKey: getString('SUPABASE_ANON_KEY') ?? '',
    mapboxAccessToken: getString('MAPBOX_ACCESS_TOKEN') ?? '',
    environment:
      (getString('ENVIRONMENT') ?? 'development') as EnvConfig['environment'],
  };
}

export const env = getEnvConfig();

export function validateEnv(): { valid: boolean; missing: string[]; warnings: string[] } {
  const missing: string[] = [];
  const warnings: string[] = [];
  const extra = Constants.expoConfig?.extra ?? {};
  const hasValue = (key: string): boolean => {
    const value = extra[key];
    return typeof value === 'string' && value.trim().length > 0;
  };

  if (!hasValue('API_URL')) {
    missing.push('API_URL');
  }
  if (!hasValue('SUPABASE_URL')) {
    warnings.push('SUPABASE_URL');
  }
  if (!hasValue('SUPABASE_ANON_KEY')) {
    warnings.push('SUPABASE_ANON_KEY');
  }
  if (!hasValue('MAPBOX_ACCESS_TOKEN')) {
    warnings.push('MAPBOX_ACCESS_TOKEN');
  }

  return {
    valid: missing.length === 0,
    missing,
    warnings,
  };
}
