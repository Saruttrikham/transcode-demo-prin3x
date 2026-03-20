'use strict';
const fs = require('fs');
const path = require('path');

const root = path.join(__dirname, '..');
const target = path.join(root, '.env');
const example = path.join(root, '.env.example');

if (fs.existsSync(target)) {
  console.log('.env already exists — not overwriting.');
  process.exit(0);
}
if (!fs.existsSync(example)) {
  console.error('Missing .env.example');
  process.exit(1);
}
fs.copyFileSync(example, target);
console.log('Created .env from .env.example — edit values as needed.');
