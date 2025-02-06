import mysql from "mysql2/promise";
import { MongoClient } from "mongodb";
import dotenv from "dotenv";

// Load biến môi trường từ .env
dotenv.config();

// Đọc cấu hình từ .env
const {
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
    MONGO_URL,
    MONGO_DATABASE,
    BATCH_SIZE,
} = process.env;

// Danh sách các bảng cần di chuyển
const tables = ["users", "orders", "products"];

// Kết nối MySQL
async function connectMySQL() {
    return mysql.createConnection({
        host: MYSQL_HOST,
        user: MYSQL_USER,
        password: MYSQL_PASSWORD,
        database: MYSQL_DATABASE,
    });
}

// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit) {
    const [rows] = await sqlConnection.execute(
        `SELECT * FROM ${tableName} LIMIT ? OFFSET ?`,
        [limit, offset]
    );
    return rows;
}

// Di chuyển dữ liệu từng batch
async function migrateTable(sqlConnection, mongoDb, tableName) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);

    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE);
        if (rows.length === 0) break;

        await mongoDb.collection(tableName).insertMany(rows);
        console.log(`✅ Đã di chuyển ${offset + rows.length} bản ghi từ bảng ${tableName}`);

        offset += BATCH_SIZE;
    }

    console.log(`🏁 Hoàn tất di chuyển bảng ${tableName}!`);
}

// Chạy quá trình di chuyển
async function migrate() {
    const sqlConnection = await connectMySQL();
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDb = mongoClient.db(MONGO_DATABASE);

    try {
        for (const table of tables) {
            await migrateTable(sqlConnection, mongoDb, table);
        }
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();
