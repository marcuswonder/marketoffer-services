import fs from "fs";
const inCsv = process.argv[2] || "airtable_schema/generic_hosts.csv";
const outJson = "config/genericHosts.json";
const lines = fs.readFileSync(inCsv, "utf-8").split(/\r?\n/).map(s => s.trim()).filter(Boolean);
const uniq = Array.from(new Set(lines));
fs.writeFileSync(outJson, JSON.stringify(uniq, null, 2));
console.log(`Wrote ${outJson} with ${uniq.length} hosts`);
