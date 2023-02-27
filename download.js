#!/usr/bin/env node

const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const { filesize } = require('filesize');

require('dotenv').config();

const {
  EC_CLOUD_ID,
  EC_API_KEY,
  BATCH_SIZE = '500',
  BATCH_LIMIT = '0',
} = process.env;

const batchSize = parseInt(BATCH_SIZE);
const batchLimit = parseInt(BATCH_LIMIT);

const EC_INDEX_NAME = process.argv[2];
const OUTPUT_FILENAME = process.argv[3];

if (!EC_INDEX_NAME || !OUTPUT_FILENAME) {
  console.log(`\nUsage: download INDEX outputFile.json\n\n`);
  return;
}

const client = new Client({
  cloud: { id: EC_CLOUD_ID },
  auth: { apiKey: EC_API_KEY },
});

const writer = fs.createWriteStream(OUTPUT_FILENAME, {});

async function run() {
  const scrollSearch = client.helpers.scrollSearch({
    index: EC_INDEX_NAME,
    size: batchSize,
  });

  const start = Date.now();
  let bytes = 0;
  let trans = 0;
  let batch = 0;
  for await (const data of scrollSearch) {
    for (let i = 0; i < data.documents.length; i++) {
      const x = JSON.stringify(data.documents[i]);
      writer.write(x);
      writer.write('\n');
      bytes += x.length + 1;
      trans++;
    }
    batch++;

    if (!(batch % 50)) {
      const elasped = (Date.now() - start) / 1000;
      console.log(`${trans}: ${elasped} ${filesize(bytes / elasped)}/s`);
    }

    if (batchLimit && batch >= batchLimit) break;
  }

  console.log();
  console.log('done.');
  console.log(`file name: ${OUTPUT_FILENAME}`);
  console.log(`  batches: ${batch}`);
  console.log(`  records: ${trans}`);
  console.log(`    bytes: ${bytes}`);
  console.log(` elaspsed: ${Date.now() - start}`);
}

run().catch(console.error);
