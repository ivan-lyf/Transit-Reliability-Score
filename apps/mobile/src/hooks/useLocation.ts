/**
 * Device location hook.
 * Requests foreground permission then starts watching position.
 */

import { useState, useEffect } from 'react';
import * as Location from 'expo-location';

export interface LocationState {
  lat: number | null;
  lon: number | null;
  error: string | null;
  loading: boolean;
}

export function useLocation(): LocationState {
  const [state, setState] = useState<LocationState>({
    lat: null,
    lon: null,
    error: null,
    loading: true,
  });

  useEffect(() => {
    let cancelled = false;

    async function getLocation(): Promise<void> {
      const { status } = await Location.requestForegroundPermissionsAsync();
      if (cancelled) return;

      if (status !== 'granted') {
        setState({ lat: null, lon: null, error: 'Location permission denied', loading: false });
        return;
      }

      try {
        const pos = await Location.getCurrentPositionAsync({
          accuracy: Location.Accuracy.Balanced,
        });
        if (!cancelled) {
          setState({
            lat: pos.coords.latitude,
            lon: pos.coords.longitude,
            error: null,
            loading: false,
          });
        }
      } catch {
        if (!cancelled) {
          setState({ lat: null, lon: null, error: 'Unable to get location', loading: false });
        }
      }
    }

    void getLocation();
    return () => {
      cancelled = true;
    };
  }, []);

  return state;
}
