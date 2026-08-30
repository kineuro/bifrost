import { defineConfig } from 'astro/config';
// Static site served by the Bifrost server from server/public.
export default defineConfig({ output: 'static', outDir: '../server/public', site: 'https://bifrost.kineuro.se', build: { format: 'directory', assets: 'assets' }, trailingSlash: 'ignore' });
