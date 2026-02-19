/**
 * Smoke test — verifies the app entry point renders without crashing.
 * The original HomeScreen was replaced by a Redirect in Stage 8.
 */

jest.mock('expo-router', () => ({
  Redirect: () => null,
  useRouter: () => ({ push: jest.fn() }),
  useLocalSearchParams: () => ({}),
  useNavigation: () => ({ setOptions: jest.fn() }),
  Stack: { Screen: () => null },
  Tabs: { Screen: () => null },
}));

jest.mock('expo-constants', () => ({
  __esModule: true,
  default: {
    expoConfig: {
      version: '0.1.0',
      extra: {
        API_URL: 'http://localhost:8000',
        MAPBOX_ACCESS_TOKEN: '',
      },
    },
  },
}));

describe('App entry', () => {
  it('exports a default component from app/index', () => {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const mod = require('../app/index') as { default: unknown };
    expect(typeof mod.default).toBe('function');
  });
});
