import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.1-dev106.0/+esm";
window.duckdb = duckdb;

const databaseUrl = 's3://duckdb-wasm-test/6e6-idx.duckdb';
let db = null;

async function initializeDatabase() {
  console.log("Initializing database...");
  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
  );

  const worker = new Worker(worker_url);
  const logger = new duckdb.ConsoleLogger();
  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(worker_url);

  await db.open({
    path: ':memory:',
    query: { castTimestampToDate: true }
  });
  console.log("Database initialized");

  const conn = await db.connect();
  console.log("Connected to database");

  try {
    await conn.query(`ATTACH '${databaseUrl}' (READ_ONLY);`);
    console.log("Remote database attached");
  } catch (error) {
    console.error("Error attaching remote database:", error);
  }
}

// Replacer function for JSON.stringify to handle BigInt
function replacer(key, value) {
  return typeof value === 'bigint' ? value.toString() : value;
}

async function runCustomQuery(query) {
  if (!db) {
    console.log("Database not initialized, initializing now...");
    await initializeDatabase();
  }

  try {
    const conn = await db.connect();
    const result = await conn.query(query);
    console.log("Query result:", result);

    // Serialize result with replacer function
    const serializedResult = JSON.stringify(result.toArray(), replacer);
    Shiny.setInputValue("duckdb_results", serializedResult);
  } catch (error) {
    console.error("Error running query:", error);
    Shiny.setInputValue("duckdb_results", JSON.stringify({ error: error.message }));
  }
}

Shiny.addCustomMessageHandler("runQuery", function(message) {
  runCustomQuery(message.query);
});
