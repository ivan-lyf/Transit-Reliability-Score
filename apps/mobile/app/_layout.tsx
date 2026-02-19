import Mapbox from '@rnmapbox/maps';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Stack } from 'expo-router';
import * as SplashScreen from 'expo-splash-screen';
import { StatusBar } from 'expo-status-bar';
import { useEffect } from 'react';
import { env } from '../src/config/env';
import { FiltersProvider } from '../src/state/FiltersProvider';

void SplashScreen.preventAutoHideAsync();

if (env.mapboxAccessToken.length > 0) {
  Mapbox.setAccessToken(env.mapboxAccessToken);
}

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 60 * 1000,
      retry: 2,
    },
  },
});

export default function RootLayout(): JSX.Element {
  useEffect(() => {
    void SplashScreen.hideAsync();
  }, []);

  return (
    <QueryClientProvider client={queryClient}>
      <FiltersProvider>
        <StatusBar style="light" />
        <Stack
          screenOptions={{
            headerStyle: { backgroundColor: '#1a1a2e' },
            headerTintColor: '#fff',
            headerTitleStyle: { fontWeight: 'bold' },
            contentStyle: { backgroundColor: '#0f172a' },
          }}
        >
          <Stack.Screen name="(tabs)" options={{ headerShown: false }} />
          <Stack.Screen name="stop/[id]" options={{ title: 'Stop Detail' }} />
        </Stack>
      </FiltersProvider>
    </QueryClientProvider>
  );
}
