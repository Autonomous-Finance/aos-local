import { aoslocal } from './src/index.js';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import fs from 'fs/promises';

const PROCESS = "Meb6GwY5I9QN77F0c5Ku2GpCFxtYyG1mfJus2GWYtII";
const MODULE = "EAIJew2R7aptjpyn7TD7S7ldVW4cTpUhZCaMvcerfWc";
const LAST_PROCESSED_FILE = 'last_processed_ids.json';

const BATCH_SIZE = 1000;
const MAX_ITERATIONS = 100;
const ITERATION_DELAY = 1000;

// Function to load last processed IDs from file
async function loadLastProcessedIds() {
    try {
        const data = await fs.readFile(LAST_PROCESSED_FILE, 'utf8');
        return JSON.parse(data);
    } catch (error) {
        return {};
    }
}

// Function to save last processed IDs to file
async function saveLastProcessedIds(ids) {
    await fs.writeFile(LAST_PROCESSED_FILE, JSON.stringify(ids, null, 2));
}

// Updated Lua code that always uses __sync_incremental_col
const dumperEval = `
local sqlite3 = require("lsqlite3")

local dump_lines = {}

-- Add the __sync_incremental_col if missing and initialize it
for row in db:nrows("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'") do
    local tbl = row.name
    
    -- Check if __sync_incremental_col exists
    local has_incremental = false
    for colinfo in db:nrows(string.format("PRAGMA table_info(%s)", tbl)) do
        if colinfo.name == "__sync_incremental_col" then
            has_incremental = true
            break
        end
    end

    -- Add and initialize the column if it doesn't exist
    if not has_incremental then
        -- Add the column
        local add_col_sql = string.format(
            "ALTER TABLE %s ADD COLUMN __sync_incremental_col INTEGER;",
            tbl
        )
        db:exec(add_col_sql)

        -- Initialize with row numbers
        local init_sql = string.format([[
            WITH numbered AS (
                SELECT rowid, ROW_NUMBER() OVER (ORDER BY rowid) as rn
                FROM %s
            )
            UPDATE %s
            SET __sync_incremental_col = (
                SELECT rn 
                FROM numbered 
                WHERE numbered.rowid = %s.rowid
            );
        ]], tbl, tbl, tbl)
        db:exec(init_sql)

        -- Create index for better performance
        local index_sql = string.format(
            "CREATE INDEX IF NOT EXISTS idx_%s_sync_inc ON %s(__sync_incremental_col);",
            tbl, tbl
        )
        db:exec(index_sql)
    end
end

-- Helper function to quote sql values
local function quote_sql_value(value)
    if value == nil then
        return "NULL"
    elseif type(value) == "number" then
        return tostring(value)
    elseif type(value) == "string" then
        local escaped = value:gsub("'", "''")
        escaped = escaped:gsub("[%c]", "")
        return string.format("'%s'", escaped)
    else
        local str = tostring(value)
        str = str:gsub("'", "''")
        str = str:gsub("[%c]", "")
        return string.format("'%s'", str)
    end
end

-- Get schema for all tables
local schema = {}
for row in db:nrows([[
    SELECT name, sql FROM sqlite_master 
    WHERE type='table' AND name NOT LIKE 'sqlite_%'
]]) do
    schema[row.name] = row.sql
end

-- Get a list of user tables
local tables = {}
for row in db:nrows("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'") do
    table.insert(tables, row.name)
end

-- Process each table
for _, tbl in ipairs(tables) do
    -- Create table if not exists
    if schema[tbl] and not last_processed_ids[tbl] then
        table.insert(dump_lines, schema[tbl] .. ";")
    end

    -- Gather columns
    local columns = {}
    local incremental_col = "__sync_incremental_col"
    local incremental_col_index = nil
    do
        local i = 0
        for colinfo in db:nrows(string.format("PRAGMA table_info(%s)", tbl)) do
            table.insert(columns, colinfo.name)
            if colinfo.name == incremental_col then
                incremental_col_index = i
            end
            i = i + 1
        end
    end

    local last_val = last_processed_ids[tbl]
    local where_clause = ""
    if last_val then
        where_clause = string.format(" WHERE %s > %s", 
            incremental_col,
            quote_sql_value(last_val)
        )
    end

    local col_list = "(" .. table.concat(columns, ", ") .. ")"
    local select_sql = string.format(
        "SELECT * FROM %s%s ORDER BY %s LIMIT %d",
        tbl,
        where_clause,
        incremental_col,
        batch_size
    )

    local stmt = db:prepare(select_sql)
    local max_val = last_val
    if stmt then
        local values_list = {}
        local fetched_rows = 0
        while stmt:step() == sqlite3.ROW do
            fetched_rows = fetched_rows + 1
            local vals = {}
            for i = 0, #columns - 1 do
                local val = stmt:get_value(i)
                table.insert(vals, quote_sql_value(val))
            end
            table.insert(values_list, "(" .. table.concat(vals, ", ") .. ")")

            local inc_val = stmt:get_value(incremental_col_index)
            if inc_val and (not max_val or inc_val > max_val) then
                max_val = inc_val
            end
        end
        stmt:finalize()

        if #values_list > 0 then
            local bulk_insert = string.format(
                "INSERT OR IGNORE INTO %s %s VALUES %s;",
                tbl,
                col_list,
                table.concat(values_list, ",\\n")
            )
            table.insert(dump_lines, bulk_insert)
        end
    end

    if not max_val then
        max_val = last_val or 0
    end
    last_processed_ids[tbl] = max_val
end


-- First export views and indexes
for row in db:nrows([[
    SELECT name, sql, type 
    FROM sqlite_master 
    WHERE type IN ('view', 'index') 
    AND name NOT LIKE 'sqlite_%'
    AND name NOT LIKE 'idx_%_sync_inc'  -- Skip our sync-specific indexes
    ORDER BY type DESC  -- Views after indexes
]]) do
    if row.sql then
        if row.type == 'index' then
            -- For indexes, add IF NOT EXISTS to prevent errors
            local modified_sql = row.sql:gsub(
                "CREATE INDEX", 
                "CREATE INDEX IF NOT EXISTS"
            )
            table.insert(dump_lines, modified_sql .. ";")
        elseif row.type == 'view' then
            -- For views, drop and recreate to avoid conflicts
            table.insert(dump_lines, string.format("DROP VIEW IF EXISTS %s;", row.name))
            table.insert(dump_lines, row.sql .. ";")
        end
    end
end


-- Output last processed IDs
for tbl, last_val in pairs(last_processed_ids) do
    table.insert(dump_lines, string.format("--LAST_PROCESSED:%s:%s", tbl, tostring(last_val)))
end



print(table.concat(dump_lines, string.char(0x1E)))
`;

function generateLuaCode(lastProcessedIds) {
    const luaEntries = Object.entries(lastProcessedIds).map(([key, value]) => {
        if (typeof value === 'string') {
            // Escape single quotes
            const safeVal = value.replace(/'/g, "\\'");
            return `    ["${key}"] = '${safeVal}'`;
        } else if (typeof value === 'number') {
            return `    ["${key}"] = ${value}`;
        } else {
            return `    ["${key}"] = nil`;
        }
    });

    return `
local sqlite3 = require("lsqlite3")

local last_processed_ids = {
${luaEntries.join(',\n')}
}

local batch_size = ${BATCH_SIZE}

${dumperEval}
`.trim();
}

async function retry(fn, retries = 3, delay = 2000) {
    for (let i = 0; i < retries; i++) {
        try {
            return await fn();
        } catch (error) {
            if (i === retries - 1) throw error;
            console.log(`Attempt ${i + 1} failed, retrying in ${delay / 1000} seconds...`);
            await new Promise(resolve => setTimeout(resolve, delay));
            delay *= 2;
        }
    }
}

async function dryRunAndUpsertToSQLite(processId) {
    let db;
    try {
        if (!processId) throw new Error('Process ID is not set.');

        db = await open({
            filename: 'local.sqlite',
            driver: sqlite3.Database
        });

        const aos = await aoslocal(MODULE);
        await aos.load(PROCESS);

        let hasMoreRecords = true;
        let iteration = 1;
        let noNewRecordsCount = 0;

        const Env = {
            Process: {
                Id: PROCESS,
                Owner: "LjFZGDae9yM-yOj0Ei7ex0xy3Zdrbn8jo-7ZqVLT19E",
                Tags: [
                    { name: "Data-Protocol", value: "ao" },
                    { name: "Variant", value: "ao.TN.1" },
                    { name: "Type", value: "Process" },
                    { name: "Authority", value: "fcoN_xJeisVsPXA-trzVAuIiqO3ydLQxM-L4XbrQKzY" }
                ],
            },
            Module: {
                Id: MODULE,
                Tags: [
                    { name: "Data-Protocol", value: "ao" },
                    { name: "Variant", value: "ao.TN.1" },
                    { name: "Type", value: "Module" }
                ]
            }
        };

        while (hasMoreRecords && iteration <= MAX_ITERATIONS) {
            try {
                console.log(`\nIteration ${iteration} of max ${MAX_ITERATIONS}`);

                const lastProcessedIds = await loadLastProcessedIds();
                const luaCode = generateLuaCode(lastProcessedIds);
                const result = await retry(() => aos.eval(luaCode, Env));

                if (result.Error) {
                    console.error('Evaluation error:', result.Error);
                    hasMoreRecords = false;
                    continue;
                }

                const sqlStatements = result.Output?.data;
                if (!sqlStatements) {
                    console.log('No data returned from evaluation');
                    hasMoreRecords = false;
                    continue;
                }

                const lines = sqlStatements.split('\x1E');
                const lastProcessedLines = lines.filter(line => line.startsWith('--LAST_PROCESSED:'));
                const statements = lines
                    .filter(line => !line.startsWith('--LAST_PROCESSED:'))
                    .join('\n')
                    .split(';')
                    .map(stmt => stmt.trim())
                    .filter(stmt => stmt.length > 0);

                let newRecordsInThisIteration = false;
                let insertCount = 0;

                for (const statement of statements) {
                    if (!statement.trim()) continue;
                    if (statement.toUpperCase().startsWith('INSERT')) {
                        insertCount++;
                        newRecordsInThisIteration = true;
                        const rowCount = (statement.match(/\),\(/g) || []).length + 1;
                        console.log(`Executing INSERT statement #${insertCount} with ${rowCount} rows`);
                    }

                    try {
                        await db.exec(statement + ';');
                    } catch (err) {
                        if (!err.message.includes('already exists')) {
                            console.error('Error executing statement:', err);
                            throw err;
                        }
                    }
                }

                if (!newRecordsInThisIteration) {
                    noNewRecordsCount++;
                    console.log(`No new records in iteration ${iteration}. Count: ${noNewRecordsCount}`);
                    if (noNewRecordsCount >= 2) {
                        console.log('No new records in consecutive iterations - sync complete');
                        hasMoreRecords = false;
                    }
                } else {
                    noNewRecordsCount = 0;
                }

                if (lastProcessedLines.length > 0) {
                    const updatedIds = { ...(await loadLastProcessedIds()) };
                    for (const line of lastProcessedLines) {
                        const [tbl, valStr] = line.replace('--LAST_PROCESSED:', '').split(':');
                        if (valStr) {
                            updatedIds[tbl] = isNaN(valStr) ? valStr : Number(valStr);
                        }
                    }
                    await saveLastProcessedIds(updatedIds);
                }

                console.log(`Iteration ${iteration} completed${hasMoreRecords ? ', continuing...' : ', finishing'}`);
                iteration++;

                if (hasMoreRecords) {
                    await new Promise(resolve => setTimeout(resolve, ITERATION_DELAY));
                }

            } catch (iterationError) {
                console.error(`Error in iteration ${iteration}:`, iterationError);
                if (iteration < MAX_ITERATIONS) {
                    await new Promise(resolve => setTimeout(resolve, 5000));
                } else {
                    hasMoreRecords = false;
                }
            }
        }

        if (iteration > MAX_ITERATIONS) {
            console.log(`Reached maximum iterations (${MAX_ITERATIONS}). Sync stopped.`);
        }

        console.log('Sync process completed');

    } catch (error) {
        console.error('Error in sync process:', error);
        throw error;
    } finally {
        if (db) {
            console.log('Closing database connection');
            await db.close();
        }
    }
}

// Run the sync
dryRunAndUpsertToSQLite(PROCESS);
