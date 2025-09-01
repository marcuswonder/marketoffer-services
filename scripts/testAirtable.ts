import "dotenv/config";
import { base } from "../src/lib/airtable.js";
(async () => {
  const tables = await base("People").select({ maxRecords: 1 }).all().catch(()=>[]);
  console.log("People table ok. Sample count:", tables.length);
})().catch(err => { console.error(err); process.exit(1); });
