module.exports = ({ config }) => {
  const existingPlugins = Array.isArray(config.plugins) ? config.plugins : [];
  const plugins = existingPlugins.includes("expo-font")
    ? existingPlugins
    : ["expo-font", ...existingPlugins];

  return {
    ...config,
    plugins,
    extra: {
      ...(config.extra ?? {}),
      API_URL: process.env.API_URL,
      SUPABASE_URL: process.env.SUPABASE_URL,
      SUPABASE_ANON_KEY: process.env.SUPABASE_ANON_KEY,
      MAPBOX_ACCESS_TOKEN: process.env.MAPBOX_ACCESS_TOKEN,
      ENVIRONMENT: process.env.ENVIRONMENT ?? (config.extra ? config.extra.ENVIRONMENT : undefined),
    },
  };
};
