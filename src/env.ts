import * as fs from 'fs';
import * as path from 'path';
import dotenv from 'dotenv';

const envPath = path.resolve(__dirname, '../.env');

// Prefer project-root .env when present for local runs.
if (fs.existsSync(envPath)) {
  dotenv.config({ path: envPath });
} else {
  dotenv.config();
}
