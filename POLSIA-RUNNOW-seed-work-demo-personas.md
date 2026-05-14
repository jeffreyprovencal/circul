# Polsia run-now: seed WORK demo personas + full chain on prod

## Overview

Run `scripts/seed-work-demo-personas.js` inside the **Render service shell** against the prod Neon DB. Source of truth: `jeffreyprovencal/circul` main, latest commit on the script file (post-`938a8e5`, includes Phase B downstream chain extension).

The script seeds the 7 WORK-DEMO-LOGINS personas (collector Naa Adjeley Lamptey, aggregator Quansah Recovery, agent Yaa Boateng, driver Selorm Agbeko, processor Sankofa Plastics, recycler Veolia Ghana, converter Alpla Group) plus a complete cross-tier transaction chain:

- Naa → Quansah: 3 collector→aggregator transactions (35.5 kg PET+HDPE)
- Quansah → Sankofa: 2 aggregator→processor dispatches (450 kg PET+HDPE) with Selorm as confirmed driver
- Sankofa → Veolia: 2 processor→recycler sales (370 kg PET+HDPE)
- Veolia → Alpla: 1 recycler→converter sale (200 kg PET pellets)
- Selorm↔Quansah `driver_aggregator_relationships` row
- Tags Naa to Vivien Luk's Impact Partner network so the IP dashboard shows kg attribution.

**Not a code deploy.** No service restart needed. The script wraps all writes in a single `BEGIN/COMMIT` transaction with `ROLLBACK` on error. It is idempotent: `ON CONFLICT DO UPDATE` on unique phone/email keys, count-of-existing guard on transactions, `ON CONFLICT DO NOTHING` on driver-link and tag rows. Safe to re-run.

**Execution constraint:** the script must run inside the **Render service shell**, NOT the agent run-now sandbox. The agent sandbox is airgapped from Neon (no outbound network to `*.neon.tech`). The Render service shell already has `DATABASE_URL` set in its env.

## Pre-run

1. Confirm `scripts/seed-work-demo-personas.js` is on the Render service file system at commit `938a8e5` or later from `jeffreyprovencal/circul main`. If not, pull/sync the deploy repo to bring it on board.
2. Open the Render service shell for the Circul prod service.
3. From repo root in that shell, confirm: `ls -la scripts/seed-work-demo-personas.js`

## Run gate

Inside the Render shell:

```
node scripts/seed-work-demo-personas.js
```

## Step A: capture script output

The script prints a self-contained verification report. Capture the entire stdout block verbatim. Expected shape:

```
=== SEED COMPLETE ===
PERSONAS (id):
  collector  Naa Adjeley Lamptey       = <int>
  aggregator Quansah Recovery (Kwesi)  = <int>
  agent      Yaa Boateng               = <int>
  driver     Selorm Agbeko             = <int>
  processor  Sankofa Plastics          = <int>
  recycler   Veolia Ghana              = <int>
  converter  Alpla Group               = <int>
CHAIN:
  Naa     → Quansah transactions : 3 (35.50 kg)
  Quansah → Sankofa dispatches   : 2 (450.00 kg, driver=Selorm)
  Sankofa → Veolia  sales        : 2 (370.00 kg)
  Veolia  → Alpla   sales        : 1 (200.00 kg)
  Selorm  ↔ Quansah driver-link  : active
IP NETWORK:
  Vivien tagged actors: <int>  (expect 1+)
```

The script's own output IS the verification — no follow-up SQL needed. The seed upserts set `is_active=true` and `must_change_pin=false` on every persona; if the script reports the ID, those flags are correct (they're in the same upsert statement). All downstream chain rows are inserted with `status='completed'`, `grade='A'`, `payment_status='paid'`, `dispatch_approved=true`.

## STOP conditions

- If the script errors with **"Vivien impact_partner record missing — seed her first via the earlier hotfix"**, STOP. Do not invent a Vivien row. Reply with the error verbatim. (Means Phase 5 IP seeding never landed on prod — separate fix.)
- If the script errors with anything else, STOP and paste the full stderr.
- If `Naa     → Quansah transactions` is anything other than `3 (35.50 kg)`, STOP and paste actual values. (Suggests a prior partial seed; do not re-run blindly.)
- If `Quansah → Sankofa dispatches` is anything other than `2 (450.00 kg, driver=Selorm)`, STOP and paste actual values.
- If `Sankofa → Veolia  sales` is anything other than `2 (370.00 kg)`, STOP and paste actual values.
- If `Veolia  → Alpla   sales` is anything other than `1 (200.00 kg)`, STOP and paste actual values.
- If `Selorm  ↔ Quansah driver-link` is anything other than `active`, STOP.
- Do NOT modify the script, the schema, or the WORK persona credentials.
- Do NOT run the script from the agent sandbox; if Render shell access isn't available, STOP and surface that.

## Reply contract

Reply with exactly this block, no narrative around it:

```
SEED RESULT:
<paste the entire === SEED COMPLETE === stdout block verbatim, all 16 lines>

EXECUTION CONTEXT:
- ran from: Render service shell
- node version: <output of `node --version`>
- commit at run time: <output of `git rev-parse HEAD`>
```

That's it. No screenshots, no walkthrough, no commentary.
