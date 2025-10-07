import fs from 'fs';
import path from 'path';
import csv from 'csv-parser';
import { Pool } from 'pg';

const DATABASE_URL = process.env.DATABASE_URL || '';
if (!DATABASE_URL) {
  console.error('Set DATABASE_URL (Postgres/Neon connection string)');
  process.exit(1);
}

const FILE_PATH = process.argv[2];
if (!FILE_PATH) {
  console.error('Usage: node --loader ts-node/esm scripts/update_ocod.ts ./reference/OCOD_CHANGES_2025_10.csv [intl]');
  process.exit(1);
}

const DATASET_LABEL = process.argv[3] || 'intl';
const SOURCE_URL = process.argv[4] || FILE_PATH;

const pool = new Pool({ connectionString: DATABASE_URL });

type RawRecord = Record<string, string>;

function normaliseAddressKey(propertyAddress: string, postcode: string) {
  const addr = (propertyAddress || '').trim().toLowerCase().replace(/\s+/g, ' ');
  const pc = (postcode || '').replace(/\s+/g, '').toLowerCase();
  return `${addr}|${pc}`;
}

function interpretAction(row: RawRecord): 'delete' | 'upsert' {
  const indicator = (row['Change Indicator'] || row['Change indicator'] || row['Change Type'] || row['Change type'] || row['Transaction'] || row['Action'] || '').trim().toLowerCase();
  if (!indicator) return 'upsert';
  if (['delete', 'deleted', 'removal', 'removed', 'ceased'].some(val => indicator.includes(val))) {
    return 'delete';
  }
  if (indicator === 'd') return 'delete';
  return 'upsert';
}

function buildOwnerRows(row: RawRecord) {
  const owners: Array<{ owner_name: string; company_number: string | null; raw: any }> = [];

  const proprietorSets: Array<[string, string, string, string, string, string, string]> = [
    [row['Proprietor Name (1)'], row['Company Registration No. (1)'], row['Proprietorship Category (1)'],
     row['Proprietor (1) Address (1)'], row['Proprietor (1) Address (2)'], row['Proprietor (1) Address (3)'],
     row['Country Incorporated (1)']],
    [row['Proprietor Name (2)'], row['Company Registration No. (2)'], row['Proprietorship Category (2)'],
     row['Proprietor (2) Address (1)'], row['Proprietor (2) Address (2)'], row['Proprietor (2) Address (3)'],
     row['Country Incorporated (2)']],
    [row['Proprietor Name (3)'], row['Company Registration No. (3)'], row['Proprietorship Category (3)'],
     row['Proprietor (3) Address (1)'], row['Proprietor (3) Address (2)'], row['Proprietor (3) Address (3)'],
     row['Country Incorporated (3)']],
    [row['Proprietor Name (4)'], row['Company Registration No. (4)'], row['Proprietorship Category (4)'],
     row['Proprietor (4) Address (1)'], row['Proprietor (4) Address (2)'], row['Proprietor (4) Address (3)'],
     row['Country Incorporated (4)']],
  ];

  proprietorSets.forEach(([name, regNo, category, addr1, addr2, addr3, country]) => {
    const trimmedName = (name || '').trim();
    if (!trimmedName) return;

    const cleanReg = (regNo || '').replace(/\s+/g, '').trim();

    owners.push({
      owner_name: trimmedName,
      company_number: cleanReg || null,
      raw: {
        name: trimmedName,
        company_number: cleanReg || null,
        category: (category || '').trim() || null,
        address_lines: [addr1, addr2, addr3].filter(Boolean),
        country_incorporated: (country || '').trim() || null,
      },
    });
  });

  return owners;
}

async function run() {
  const client = await pool.connect();
  let processed = 0;
  let deletes = 0;
  let upserts = 0;
  let ownersInserted = 0;
  const start = Date.now();

  try {
    await client.query('BEGIN');
    console.log(`Starting OCOD change import (${DATASET_LABEL}) from ${SOURCE_URL}`);

    const stream = fs.createReadStream(path.resolve(FILE_PATH)).pipe(csv());

    for await (const row of stream) {
      if (row['Title Number'] && row['Title Number'].startsWith('Row Count')) {
        continue;
      }

      processed += 1;
      const action = interpretAction(row);
      const addressKey = normaliseAddressKey(row['Property Address'] || '', row['Postcode'] || '');
      if (!addressKey.trim()) {
        continue;
      }

      if (action === 'delete') {
        await client.query(
          'DELETE FROM land_registry_corporate WHERE dataset_label = $1 AND address_key = $2',
          [DATASET_LABEL, addressKey]
        );
        deletes += 1;
      } else {
        const owners = buildOwnerRows(row);
        await client.query(
          'DELETE FROM land_registry_corporate WHERE dataset_label = $1 AND address_key = $2',
          [DATASET_LABEL, addressKey]
        );
        if (owners.length) {
          ownersInserted += await insertOwners(client, owners, addressKey, row);
          upserts += 1;
        }
      }

      if (processed % 1000 === 0) {
        console.log(`Processed ${processed.toLocaleString()} change rows (upserts: ${upserts}, deletes: ${deletes})`);
      }
    }

    await client.query(
      `INSERT INTO land_registry_corporate_meta (dataset_label, last_refreshed_at, source_url, row_count)
       VALUES ($1, now(), $2, (SELECT COUNT(*) FROM land_registry_corporate WHERE dataset_label = $1))
       ON CONFLICT (dataset_label)
         DO UPDATE SET last_refreshed_at = EXCLUDED.last_refreshed_at,
                       source_url = EXCLUDED.source_url,
                       row_count = EXCLUDED.row_count`,
      [DATASET_LABEL, SOURCE_URL]
    );

    await client.query('COMMIT');
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    console.log(`OCOD changes applied. Rows processed: ${processed.toLocaleString()} (upserts: ${upserts}, deletes: ${deletes}, owners inserted: ${ownersInserted.toLocaleString()}) in ${elapsed}s.`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('OCOD change import failed:', err);
  } finally {
    client.release();
    await pool.end();
  }
}

async function insertOwners(client: any, owners: Array<{ owner_name: string; company_number: string | null; raw: any }>, addressKey: string, row: RawRecord) {
  const values: string[] = [];
  const params: any[] = [];

  owners.forEach((owner, index) => {
    const base = index * 5;
    values.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`);
    params.push(
      addressKey,
      owner.owner_name,
      owner.company_number ?? null,
      DATASET_LABEL,
      JSON.stringify({
        change_indicator: row['Change Indicator'] || row['Change indicator'] || row['Change Type'] || row['Transaction'] || null,
        title_number: row['Title Number'] || null,
        tenure: row['Tenure'] || null,
        property_address: row['Property Address'] || null,
        district: row['District'] || null,
        county: row['County'] || null,
        region: row['Region'] || null,
        postcode: row['Postcode'] || null,
        price_paid: row['Price Paid'] || null,
        proprietor: owner.raw,
        date_proprietor_added: row['Date Proprietor Added'] || null,
        additional_proprietor_indicator: row['Additional Proprietor Indicator'] || null,
      })
    );
  });

  if (!values.length) return 0;

  await client.query(
    `INSERT INTO land_registry_corporate (address_key, owner_name, company_number, dataset_label, raw)
     VALUES ${values.join(',')}
     ON CONFLICT (address_key, owner_name, dataset_label) DO NOTHING`,
    params
  );

  return owners.length;
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
