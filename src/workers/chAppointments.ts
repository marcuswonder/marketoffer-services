import "dotenv/config";
import { Worker } from "bullmq";
import { connection } from "../queues/index.js";
import { httpGetJson } from "../lib/http.js";
import { parseOfficerName, officerIdFromUrl, monthStr } from "../lib/normalize.js";
import { batchCreate } from "../lib/airtable.js";
import { logger } from "../lib/logger.js";

const CH_BASE = process.env.CH_API_BASE || "https://api.company-information.service.gov.uk";
const CH_KEY = process.env.CH_API_KEY || "";

function chHeaders() {
  const auth = Buffer.from(`${CH_KEY}:`).toString("base64");
  return { Authorization: `Basic ${auth}` };
}

type CHOfficer = {
  name: string;
  officer_role: string;
  date_of_birth?: { month: number; year: number };
  links?: { officer?: { appointments?: string } };
};

export default new Worker("ch:appointments", async job => {
  const { companyNumber } = job.data as { companyNumber: string };

  const company = await httpGetJson<any>(`${CH_BASE}/company/${companyNumber}`, { headers: chHeaders() });
  const officers = await httpGetJson<any>(`${CH_BASE}/company/${companyNumber}/officers`, { headers: chHeaders() });

  const peopleRecords:any[] = [];
  const appointmentRecords:any[] = [];

  for (const o of (officers.items || []) as CHOfficer[]) {
    if (!o.name) continue;
    const role = (o.officer_role || "").toLowerCase();
    if (!role.includes("director")) continue;

    const { first, middle, last } = parseOfficerName(o.name);
    const dob = o.date_of_birth;
    const dobString = dob ? `${monthStr(dob.month)} ${dob.year}` : "";

    const officerUrl = (o.links?.officer?.appointments || "") as string;
    const officerId = officerIdFromUrl(officerUrl) || "";

    // People
    peopleRecords.push({
      fields: {
        first_name: first,
        middle_names: middle,
        last_name: last,
        dob_year: dob?.year || null,
        dob_month: dob?.month || null,
        dob_string: dobString,
        officer_ids: [ officerId ].filter(Boolean),
        status: "Found via CH"
      }
    });

    // Appointments
    appointmentRecords.push({
      fields: {
        company_number: companyNumber,
        company_name: company.company_name || "",
        officer_role: o.officer_role || "",
        officer_id: officerId
      }
    });
  }

  if (peopleRecords.length) await batchCreate("People", peopleRecords);
  if (appointmentRecords.length) await batchCreate("Appointments", appointmentRecords);

  logger.info({ companyNumber, people: peopleRecords.length, appointments: appointmentRecords.length }, "CH processed");
}, { connection, concurrency: 2 });
