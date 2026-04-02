module.exports = {
  name: 'reset_expense_categories',
  up: async (client) => {
    // First check if any expense_entries reference existing categories
    const hasEntries = await client.query(
      `SELECT COUNT(*) AS count FROM expense_entries`
    );

    if (parseInt(hasEntries.rows[0].count) > 0) {
      // If there are existing entries, we need to remap them.
      // Create a mapping from old category names to new ones.
      // Get current categories first.
      const oldCats = await client.query(`SELECT id, name FROM expense_categories`);
      const nameMap = {
        'Transport & Logistics': 'Transportation',
        'Administration': 'Transportation',  // closest match
        'Equipment & Tools': 'Equipment',
        'Facility & Rent': 'Storage',         // closest match for informal context
        'Staff & Labour': 'Labour',
        'Utilities': 'Fuel',                  // closest match for informal context
        'Other': 'Mobile money fees'           // catch-all remap
      };

      // Delete all current categories (will fail if FK — so we update in place)
      // Strategy: rename existing categories to match the new names
      for (const old of oldCats.rows) {
        const newName = nameMap[old.name] || old.name;
        await client.query(
          `UPDATE expense_categories SET name = $1, status = 'default' WHERE id = $2`,
          [newName, old.id]
        );
      }

      // Now ensure we have exactly the 7 we need (insert any missing)
      const targets = ['Transportation', 'Fuel', 'Storage', 'Labour', 'Equipment', 'Maintenance', 'Mobile money fees'];
      const current = await client.query(`SELECT name FROM expense_categories`);
      const currentNames = current.rows.map(r => r.name);
      for (const t of targets) {
        if (!currentNames.includes(t)) {
          await client.query(
            `INSERT INTO expense_categories (name, status) VALUES ($1, 'default')`,
            [t]
          );
        }
      }

      // Remove any categories that aren't in our target list AND have no entries
      await client.query(`
        DELETE FROM expense_categories
        WHERE name != ALL($1::text[])
        AND id NOT IN (SELECT DISTINCT category_id FROM expense_entries)
      `, [targets]);

    } else {
      // No entries — safe to truncate and re-seed
      await client.query(`TRUNCATE expense_categories RESTART IDENTITY CASCADE`);
      await client.query(`
        INSERT INTO expense_categories (name, status) VALUES
          ('Transportation', 'default'),
          ('Fuel', 'default'),
          ('Storage', 'default'),
          ('Labour', 'default'),
          ('Equipment', 'default'),
          ('Maintenance', 'default'),
          ('Mobile money fees', 'default')
      `);
    }
  }
};
