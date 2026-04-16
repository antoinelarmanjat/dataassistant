import { defineConfig } from 'vite';
import baseConfig from './vite.config';

export default defineConfig(async () => {
  const config = await baseConfig();
  // Override buildup to only build index.html
  if (config.build && config.build.rollupOptions) {
    config.build.rollupOptions.input = { contact: config.build.rollupOptions.input['contact'] };
  }
  return config;
});
