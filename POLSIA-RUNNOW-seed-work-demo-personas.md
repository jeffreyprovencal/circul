# Polsia run-now: deploy WORK demo seed via migration

## Overview

Sync + deploy `jeffreyprovencal/circul` main. The latest commit adds a new migration `migrations/1779000000004_seed_work_demo_personas.js` that seeds the 7 WORK-DEMO-LOGINS personas (Naa Adjeley Lamptey / Quansah Recovery / Yaa Boateng / Selorm Agbeko / Sankofa Plastics / Veolia / Alpla) plus a full cross-tier chain (collector → aggregator → processor → recycler → converter, with Selorm as driver) and tags Naa to Vivien Luk's Impact Partner network.

**Execution path:** the seed runs via the standard migration runner on deploy (`npm run build` → `npm run migrate` → `node migrate.js`). The migration is wrapped in `BEGIN/COMMIT` by the runner and tracked in `_migrations` by name (`seed_work_demo_personas`), so it runs exactly once per database.

**Why a migration not a Render shell run:** the prior attempt (#1582746) discovered the polsia_infra toolset has no shell execution capability. Reframing as a migration uses the canonical Polsia deploy pipeline.

**Idempotency:** the migration body uses `ON CONFLICT DO UPDATE` on persona unique keys and count-of-existing guards on every chain segment. Re-running the same body manually (via psql if ever needed) is safe.

## Pre-deploy

1. Pull `jeffreyprovencal/circul` main into the deploy repo. Confirm tip includes the new migration file: `ls migrations/1779000000004_seed_work_demo_personas.js`.

## Deploy gate

2. Trigger the standard sync + deploy to Render. The build step runs `npm run migrate` which executes pending migrations in order. The `seed_work_demo_personas` migration is the only new one and will run automatically.

## Step A: verify migration tracking

Use `query_db` to confirm the migration recorded in `_migrations`:

```sql
SELECT name, applied_at FROM _migrations
WHERE name = 'seed_work_demo_personas';
```

Expect exactly one row with `applied_at` set to deploy time.

## Step B: verify seeded data

Use `query_db` to confirm the chain populated correctly. Three queries:

```sql
-- Q1: persona presence
SELECT 'collector'   AS tier, id, first_name || ' ' || last_name AS name FROM collectors WHERE phone='0241555001'
UNION ALL SELECT 'aggregator', id, name FROM aggregators WHERE phone='0241555002'
UNION ALL SELECT 'agent',      id, first_name || ' ' || last_name FROM agents WHERE phone='0241555003'
UNION ALL SELECT 'driver',     id, first_name || ' ' || last_name FROM drivers WHERE phone='0241555004'
UNION ALL SELECT 'processor',  id, name FROM processors WHERE email='sankofa@circul.demo'
UNION ALL SELECT 'recycler',   id, name FROM recyclers  WHERE email='veolia@circul.demo'
UNION ALL SELECT 'converter',  id, name FROM converters WHERE email='alpla@circul.demo';
```

Expect 7 rows.

```sql
-- Q2: chain segments
SELECT 'naa→quansah'      AS segment, COUNT(*)::int AS n, COALESCE(SUM(gross_weight_kg),0)::numeric AS kg
  FROM transactions WHERE collector_id = (SELECT id FROM collectors WHERE phone='0241555001')
                      AND aggregator_id = (SELECT id FROM aggregators WHERE phone='0241555002')
UNION ALL
SELECT 'quansah→sankofa', COUNT(*)::int, COALESCE(SUM(gross_weight_kg),0)::numeric
  FROM pending_transactions WHERE transaction_type='aggregator_sale'
                              AND aggregator_id = (SELECT id FROM aggregators WHERE phone='0241555002')
                              AND processor_id  = (SELECT id FROM processors  WHERE email='sankofa@circul.demo')
UNION ALL
SELECT 'sankofa→veolia',  COUNT(*)::int, COALESCE(SUM(gross_weight_kg),0)::numeric
  FROM pending_transactions WHERE transaction_type='processor_sale'
                              AND processor_id = (SELECT id FROM processors WHERE email='sankofa@circul.demo')
                              AND recycler_id  = (SELECT id FROM recyclers  WHERE email='veolia@circul.demo')
UNION ALL
SELECT 'veolia→alpla',    COUNT(*)::int, COALESCE(SUM(gross_weight_kg),0)::numeric
  FROM pending_transactions WHERE transaction_type='recycler_sale'
                              AND recycler_id  = (SELECT id FROM recyclers  WHERE email='veolia@circul.demo')
                              AND converter_id = (SELECT id FROM converters WHERE email='alpla@circul.demo');
```

Expect exactly:
- naa→quansah:      3 rows / 35.50 kg
- quansah→sankofa:  2 rows / 450.00 kg
- sankofa→veolia:   2 rows / 370.00 kg
- veolia→alpla:     1 row  / 200.00 kg

```sql
-- Q3: relationships
SELECT 'driver_link' AS check, status AS value FROM driver_aggregator_relationships
WHERE driver_id     = (SELECT id FROM drivers WHERE phone='0241555004')
  AND aggregator_id = (SELECT id FROM aggregators WHERE phone='0241555002')
UNION ALL
SELECT 'vivien_tag',
       CASE WHEN COUNT(*) > 0 THEN 'tagged' ELSE 'missing' END
FROM impact_partner_actor_tags
WHERE impact_partner_id = (SELECT id FROM impact_partners WHERE email='vivien@work.global')
  AND actor_type='collector'
  AND actor_id = (SELECT id FROM collectors WHERE phone='0241555001')
  AND deactivated_at IS NULL;
```

Expect:
- driver_link: `active`
- vivien_tag:  `tagged`

## STOP conditions

- If deploy fails: STOP and paste the build log tail (last ~30 lines).
- If `_migrations` does NOT contain `seed_work_demo_personas` after deploy: STOP. (Means the migration didn't run — investigate why before retrying.)
- If Q1 returns fewer than 7 rows: STOP. Paste the actual rows.
- If Q2 row counts ≠ `3 / 2 / 2 / 1` or kg sums ≠ `35.50 / 450.00 / 370.00 / 200.00`: STOP. Paste actual values.
- If Q3 driver_link ≠ `active` or vivien_tag ≠ `tagged`: STOP. Paste actual values.
- Do NOT manually re-run the migration or attempt to undo and redo. Migration-runner state is the source of truth.

## Reply contract

Reply with exactly this block, no narrative:

```
DEPLOY: <success | failed>
COMMIT DEPLOYED: <short sha>

MIGRATION TRACKING:
<Q1 result rows verbatim>

CHAIN:
<Q2 result rows verbatim>

RELATIONSHIPS:
<Q3 result rows verbatim>
```

That's it. No screenshots, no walkthrough, no commentary.
