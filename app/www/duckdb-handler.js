let db;
const databaseUrl = 's3://duckdb-wasm-test/6e6-idx.duckdb';

function waitForDuckDB(callback) {
  if (window.duckdbWasmLoaded && window.duckdb) {
    console.log("DuckDB WASM is loaded and available");
    callback();
  } else {
    console.log("Waiting for DuckDB WASM to load...");
    setTimeout(() => waitForDuckDB(callback), 100);
  }
}

Shiny.addCustomMessageHandler("runQuery", function(message) {
  console.log("Received query:", message.query);
  waitForDuckDB(() => runQuery(message.query));
});

async function initializeDatabase() {
  console.log("Initializing database...");
  if (!db) {
    try {
      const JSDELIVR_BUNDLES = window.duckdb.getJsDelivrBundles();
      const bundle = await window.duckdb.selectBundle(JSDELIVR_BUNDLES);

      const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}");`], {type: 'text/javascript'})
      );

      const worker = new Worker(worker_url);
      const logger = new window.duckdb.ConsoleLogger();
      db = new window.duckdb.AsyncDuckDB(logger, worker);
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      URL.revokeObjectURL(worker_url);

      console.log("DuckDB instantiated");
      
      await db.open({
        path: ':memory:',
        query: { castTimestampToDate: true }
      });
      console.log("Database opened");
      
      const conn = await db.connect();
      console.log("Connected to database");
      
      await conn.query(`INSTALL httpfs; LOAD httpfs;`);
      console.log("HTTP FS installed and loaded");
      
      await conn.query(`ATTACH '${databaseUrl}' AS remote_db;`);
      console.log("Remote database attached");
      
      Shiny.setInputValue("duckdb_message", "Database initialized and connected to remote file");
    } catch (e) {
      console.error("Error initializing database:", e);
      Shiny.setInputValue("duckdb_message", "Error initializing database: " + e.message);
    }
  }
}

async function runQuery(query) {
  console.log("Running query:", query);
  if (!db) {
    console.log("Database not initialized, initializing now...");
    await initializeDatabase();
  }

  try {
    const conn = await db.connect();
    console.log("Connected to database, executing query...");
    const result = await conn.query(query);
    console.log("Query executed, result:", result);
    Shiny.setInputValue("duckdb_results", JSON.stringify(result.toArray()));
    Shiny.setInputValue("duckdb_message", "Query executed successfully");
  } catch (e) {
    console.error("Error executing query:", e);
    Shiny.setInputValue("duckdb_message", "Error executing query: " + e.message);
  }
}
