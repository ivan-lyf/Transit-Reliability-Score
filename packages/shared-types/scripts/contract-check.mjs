import { createRequire } from 'module';

const require = createRequire(import.meta.url);

const cjsModule = require('@transit/shared-types');
if (!Array.isArray(cjsModule.DAY_TYPES)) {
  throw new Error('CJS export check failed: DAY_TYPES not found.');
}

const esmModule = await import('@transit/shared-types');
const dayTypes = esmModule.DAY_TYPES ?? esmModule.default?.DAY_TYPES;
if (!Array.isArray(dayTypes)) {
  throw new Error('ESM export check failed: DAY_TYPES not found.');
}

console.log('Shared types contract check passed.');
